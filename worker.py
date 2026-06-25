import boto3
import json
import subprocess
import os
import shutil
import time
import tempfile
import threading
import uuid
import zipfile
from supabase import create_client, Client

# ==========================================
# CONFIGURAZIONE
# ==========================================

ssm = boto3.client('ssm', region_name='eu-north-1')

def get_parameter(name):
    response = ssm.get_parameter(Name=name, WithDecryption=True)
    return response['Parameter']['Value']

# Percorso per i file temporanei su volume EBS espanso
HLS_DIR = "/mnt/data/hls_temp"
os.makedirs(HLS_DIR, exist_ok=True)

# Inizializzazione
AWS_REGION = get_parameter("REGION")
SQS_QUEUE_URL = get_parameter("SQS_QUEUE_URL")
S3_BUCKET = get_parameter("S3_BUCKET")
SUPABASE_URL = get_parameter("SUPABASE_URL")
SUPABASE_KEY = get_parameter("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
s3_client = boto3.client('s3', region_name=AWS_REGION)
sqs_client = boto3.client('sqs', region_name=AWS_REGION)

def update_video_status(video_id, status):
    """Aggiorna lo stato del video nella tabella 'videos' di Supabase."""
    try:
        supabase.table("videos").update({"status": status}).eq("id", video_id).execute()
        print(f"[{video_id}] Stato aggiornato in Supabase a: {status}")
    except Exception as e:
        print(f"[{video_id}] Errore aggiornamento Supabase: {e}")
        
def update_clip_status(clip_id, status):
    """Aggiorna lo stato della clip nella tabella 'clips' di Supabase."""
    try:
        supabase.table("clips").update({"status": status}).eq("id", clip_id).execute()
        print(f"[{video_id}] Stato aggiornato in Supabase a: {status}")
    except Exception as e:
        print(f"[{video_id}] Errore aggiornamento Supabase: {e}")

def get_presigned_url(bucket, key, expiration=3600):
    """Genera un URL temporaneo per far leggere il file a FFmpeg direttamente da S3."""
    return s3_client.generate_presigned_url('get_object',
                                            Params={'Bucket': bucket, 'Key': key},
                                            ExpiresIn=expiration)
    
def check_media_codecs(stream_url):
    """
    Analizza il file multimediale usando ffprobe e restituisce i codec video e audio.
    Ritorna una tupla: (video_codec, audio_codec)
    """
    try:
        cmd = [
            'ffprobe',
            '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'stream=codec_name',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            stream_url
        ]
        v_result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        video_codec = v_result.stdout.strip()

        cmd_audio = [
            'ffprobe',
            '-v', 'error',
            '-select_streams', 'a:0',
            '-show_entries', 'stream=codec_name',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            stream_url
        ]
        a_result = subprocess.run(cmd_audio, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        audio_codec = a_result.stdout.strip()
        
        return video_codec, audio_codec
    except Exception as e:
        print(f"Errore durante ffprobe: {e}")
        return None, None
            
def upload_and_cleanup(tmp_dir, video_id, stop_event):
    """
    Monitora la directory. Legge il file m3u8 per sapere quali segmenti .ts 
    sono completi. Li carica su S3 e li elimina istantaneamente per salvare spazio.
    """
    uploaded = set()
    index_path = os.path.join(tmp_dir, 'index.m3u8')
    
    while not stop_event.is_set():
        if os.path.exists(index_path):
            try:
                with open(index_path, 'r') as f:
                    m3u8_content = f.read()
                
                for filename in os.listdir(tmp_dir):
                    # Se è un segmento .ts e il suo nome compare nel file m3u8, 
                    # significa che FFmpeg ha chiuso il file ed è sicuro caricarlo.
                    if filename.endswith(".ts") and filename not in uploaded:
                        if filename in m3u8_content:
                            local_path = os.path.join(tmp_dir, filename)
                            s3_dest_key = f"{video_id}/hls/{filename}"
                            try:
                                s3_client.upload_file(local_path, S3_BUCKET, s3_dest_key)
                                os.remove(local_path)  # Pulizia immediata
                                uploaded.add(filename)
                            except Exception as e:
                                print(f"[{video_id}] Errore upload parziale {filename}: {e}")
            except Exception as e:
                pass # Ignoriamo errori temporanei di lettura file
        
        time.sleep(1) # Attesa per non saturare la CPU
            
def process_generate_hls(video_id, s3_source):
    print(f"[{video_id}] Inizio processo HLS...")
    tmp_dir = f"{HLS_DIR}/hls_{video_id}"
    os.makedirs(tmp_dir, exist_ok=True)
    
    # Estrai l'estensione originale dal path S3 per mantenere la massima compatibilità
    _, ext = os.path.splitext(s3_source)
    file_ext = ext if ext else ".mp4" # Fallback a .mp4 se l'estensione è assente
    
    # Crea un file temporaneo locale per scaricare il sorgente (Risolve l'errore "not seekable")
    with tempfile.NamedTemporaryFile(suffix=file_ext, dir=HLS_DIR) as local_video:
        print(f"[{video_id}] Download video originale in {local_video.name}...")
        print(f"[{video_id}] s3_source {s3_source}")
        
        s3_client.download_file(S3_BUCKET, s3_source, local_video.name)
        
        # 1. Controlla i codec del file LOCALE appena scaricato
        raw_video, raw_audio = check_media_codecs(local_video.name)
        video_codec = raw_video.split('\n')[0].strip()
        audio_codec = raw_audio.split('\n')[0].strip()
        print(f"[{video_id}] Codec rilevati -> Video: {video_codec}, Audio: {audio_codec}")
        
        # 2. Configura FFmpeg
        index_file = os.path.join(tmp_dir, 'index.m3u8')
        ffmpeg_cmd = [
            'ffmpeg', '-i', local_video.name, 
            '-start_number', '0', '-hls_time', '10', 
            '-hls_list_size', '0', '-f', 'hls'
        ]
        
        # Gestione Sicura dei Codec (Incluso il caso "unknown" o fallimenti di ffprobe)
        if video_codec in ['h264', 'hevc']:
            print(f"[{video_id}] Video nativo (H.264). Uso copy (ultra-veloce).")
            ffmpeg_cmd.extend(['-c:v', 'copy'])
        else:
            print(f"[{video_id}] Codec video '{video_codec}' rilevato. Forzo transcodifica in H.264...")
            # Usa 'fast' o 'veryfast' per non saturare la CPU della EC2.
            # -crf 23 mantiene una buona qualità visiva tenendo bassi i pesi.
            ffmpeg_cmd.extend(['-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23'])
        
        if audio_codec in ['aac', 'mp3']:
            print(f"[{video_id}] Audio nativo (AAC). Uso copy.")
            ffmpeg_cmd.extend(['-c:a', 'copy'])
        else:
            print(f"[{video_id}] Codec audio '{audio_codec}' rilevato. Forzo transcodifica in AAC...")
            # Forziamo aac a 128k, lo standard per il web
            ffmpeg_cmd.extend(['-c:a', 'aac', '-b:a', '128k'])
            
        ffmpeg_cmd.append(index_file)
        
        # 3. Avvia il thread di Upload e Pulizia
        stop_event = threading.Event()
        upload_thread = threading.Thread(target=upload_and_cleanup, args=(tmp_dir, video_id, stop_event))
        upload_thread.start()
        
        try:
            # 4. Lancia FFmpeg
            print(f"[{video_id}] Lancio FFmpeg in corso...")
            subprocess.run(ffmpeg_cmd, check=True)
            print(f"[{video_id}] FFmpeg ha completato la conversione.")
            
        finally:
            # 5. Ferma il thread asincrono
            stop_event.set()
            upload_thread.join()
            
            # 6. Carica tutto ciò che è rimasto (es. l'index.m3u8 finale e gli ultimissimi .ts)
            print(f"[{video_id}] Upload finale file residui...")
            if os.path.exists(tmp_dir):
                for filename in os.listdir(tmp_dir):
                    local_path = os.path.join(tmp_dir, filename)
                    s3_dest_key = f"{video_id}/hls/{filename}"
                    try:
                        s3_client.upload_file(local_path, S3_BUCKET, s3_dest_key)
                    except Exception as e:
                        print(f"[{video_id}] Errore upload finale {filename}: {e}")
                
                # 7. Rimuovi la directory temporanea
                shutil.rmtree(tmp_dir)
                print(f"[{video_id}] Pulizia disco completata.")
    
def process_extract_clip(clip_id, s3_source, video_id):
    """Estrae una clip video frame-accurate partendo da un timestamp e la carica su S3."""
    print(f"[{clip_id}] Inizio estrazione clip del video {video_id}, presente al path {s3_source}...")
    
    try:
        # Query per recuperare i parametri necessari
        response = supabase.table("clips").select("title, anchor_time, offset_start, offset_end").eq("id", clip_id).single().execute()
        data = response.data
        
        anchor_time = data['anchor_time'] / 1000.0
        print(f"anchor_time: {anchor_time}")
        pre_buffer = data['offset_start']
        print(f"pre_buffer: {pre_buffer}")
        post_buffer = data['offset_end']
        print(f"post_buffer: {post_buffer}")
        title = data["title"]
        
        start_time = max(0, anchor_time - pre_buffer)
        print(f"start_time: {start_time}")
        duration = pre_buffer + post_buffer
        print(f"duration: {duration}")
        
        local_output = f"/mnt/data/clip_temp/clip_{clip_id}.mp4"
        
        stream_url = get_presigned_url(S3_BUCKET, s3_source)
        
        # 1. Controlla i codec del file LOCALE appena scaricato
        raw_video, raw_audio = check_media_codecs(stream_url)
        video_codec = raw_video.split('\n')[0].strip()
        audio_codec = raw_audio.split('\n')[0].strip()
        print(f"[{video_id}] Codec rilevati -> Video: {video_codec}, Audio: {audio_codec}")
        
        # Esecuzione estrazione
        ffmpeg_cmd = [
            'ffmpeg', 
            '-ss', str(start_time),
            '-t', str(duration),
            '-i', stream_url
        ]
        
        # Gestione Sicura dei Codec (Incluso il caso "unknown" o fallimenti di ffprobe)
        if video_codec in ['h264', 'hevc']:
            print(f"[{video_id}] Video nativo (H.264). Uso copy (ultra-veloce).")
            ffmpeg_cmd.extend(['-c:v', 'copy'])
        else:
            print(f"[{video_id}] Codec video '{video_codec}' rilevato. Forzo transcodifica in H.264...")
            # Usa 'fast' o 'veryfast' per non saturare la CPU della EC2.
            # -crf 23 mantiene una buona qualità visiva tenendo bassi i pesi.
            ffmpeg_cmd.extend(['-c:v', 'libx264', '-preset', 'veryfast'])
        
        if audio_codec in ['aac', 'mp3']:
            print(f"[{video_id}] Audio nativo (AAC). Uso copy.")
            ffmpeg_cmd.extend(['-c:a', 'copy'])
        else:
            print(f"[{video_id}] Codec audio '{audio_codec}' rilevato. Forzo transcodifica in AAC...")
            # Forziamo aac a 128k, lo standard per il web
            ffmpeg_cmd.extend(['-c:a', 'aac', '-b:a', '128k'])
            
        ffmpeg_cmd.extend(['-y', local_output])
        subprocess.run(ffmpeg_cmd, check=True)
        
        # Upload
        s3_dest_key = f"{video_id}/clips/{title}.mp4"
        s3_client.upload_file(local_output, S3_BUCKET, s3_dest_key)
        
        # Aggiornamento stato su Supabase
        supabase.table("clips").update({"status": "CLIPPED", "s3_source": s3_dest_key}).eq("id", clip_id).execute()
        print(f"[{clip_id}] Clip estratta e caricata con successo.")
        
    except Exception as e:
        print(f"[{clip_id}] Errore durante l'estrazione: {e}")
        supabase.table("clips").update({"status": "ERROR"}).eq("id", clip_id).execute()
    finally:
        if os.path.exists(local_output):
            os.remove(local_output)

def process_create_zip(video_id, clip_ids):
    """
    Scarica un array di clip da S3, le comprime in un file ZIP una alla volta (cancellandole),
    carica il file ZIP su S3 e aggiorna lo stato su Supabase.
    """
    zip_id = str(uuid.uuid4())
    print(f"[{video_id}] Inizio creazione ZIP {zip_id} per {len(clip_ids)} clip...")
    
    # 1. Crea la riga su Supabase in stato CREATED
    try:
        supabase.table('zip_files').insert({
            'id': zip_id,
            'status': 'CREATED'
        }).execute()
    except Exception as e:
        print(f"[{video_id}] Errore durante la creazione del record zip_files: {e}")
        return

    tmp_dir = f"/mnt/data/clip_temp/zip_{zip_id}"
    os.makedirs(tmp_dir, exist_ok=True)
    local_zip_path = os.path.join(tmp_dir, f"zip_{zip_id}.zip")
    
    try:
        # 2. Query su Supabase per leggere i s3_source delle clip
        # Assicurati che il nome della tabella sia corretto (es. 'clips')
        response = supabase.table('clips').select('s3_source').in_('id', clip_ids).execute()
        clips_data = response.data
        
        if not clips_data:
            raise Exception("Nessuna clip trovata su Supabase per gli ID forniti.")

        # 3. Crea lo ZIP scaricando e comprimendo una clip alla volta
        with zipfile.ZipFile(local_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for clip in clips_data:
                s3_source = clip.get('s3_source')
                if not s3_source:
                    continue
                
                # Estrai il nome del file (es. clip_123.mp4) e rimuovi eventuale prefisso s3://
                clip_filename = s3_source.split('/')[-1]
                s3_key = s3_source.replace(f"s3://{S3_BUCKET}/", "")
                local_clip_path = os.path.join(tmp_dir, clip_filename)
                
                print(f"[{video_id}] Download clip {clip_filename} per ZIP...")
                s3_client.download_file(S3_BUCKET, s3_key, local_clip_path)
                
                zipf.write(local_clip_path, arcname=clip_filename)
                print(f"[{video_id}] Aggiunta {clip_filename} allo ZIP...")
                
                # Elimina istantaneamente la clip scaricata per non saturare il disco
                os.remove(local_clip_path)
        
        # 4. Upload dello ZIP finale su S3
        s3_dest_key = f"{video_id}/clip/zip_{zip_id}.zip"
        print(f"[{video_id}] Upload ZIP finale su S3 in {s3_dest_key}...")
        s3_client.upload_file(local_zip_path, S3_BUCKET, s3_dest_key)
        
        # 5. Aggiorna Supabase a READY
        supabase.table('zip_files').update({'status': 'READY'}).eq('id', zip_id).execute()
        print(f"[{video_id}] Creazione ZIP {zip_id} completata con successo!")

    except Exception as e:
        print(f"[{video_id}] Errore durante la creazione dello ZIP: {e}")
        # Aggiorna Supabase a ERROR in caso di fallimento
        supabase.table('zip_files').update({'status': 'ERROR'}).eq('id', zip_id).execute()
        
    finally:
        # 6. Pulizia disco: elimina l'intera cartella temporanea e il file zip locale
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
            print(f"[{video_id}] Pulizia disco completata per ZIP {zip_id}.")

def main():
    print("Worker EC2 avviato. In attesa di messaggi SQS...")
    
    while True:
        # Long Polling: aspetta 20 secondi se la coda è vuota (fa risparmiare chiamate API)
        response = sqs_client.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20 
        )
        
        messages = response.get('Messages', [])
        if not messages:
            continue
            
        for msg in messages:
            try:
                body = json.loads(msg['Body'])
                job_type = body.get('job_type')
                video_id = body.get('video_id')
                
                if job_type == 'generate_hls':
                    process_generate_hls(video_id, body['s3_source'])
                    
                elif job_type == 'extract_clip':
                    process_extract_clip(
                        clip_id=body['clip_id'],
                        s3_source=body['s3_source'],
                        video_id=video_id
                    )
                elif job_type == 'create_zip':
                    process_create_zip(
                        clip_ids=body['clip_ids'],
                        video_id=video_id
                    )
                else:
                    print(f"Job_type sconosciuto: {job_type}")
                    
                # Se tutto è andato bene, cancella il messaggio dalla coda
                sqs_client.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=msg['ReceiptHandle']
                )
                print("Messaggio rimosso dalla coda con successo.\n---")
                
            except Exception as e:
                # Se c'è un errore (es. FFmpeg fallisce), il messaggio NON viene cancellato.
                # Tornerà visibile in coda dopo il Visibility Timeout.
                print(f"ERRORE durante l'elaborazione: {e}")

if __name__ == "__main__":
    main()