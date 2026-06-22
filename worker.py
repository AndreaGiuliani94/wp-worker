import boto3
import json
import subprocess
import os
import shutil
import time
from supabase import create_client, Client

# ==========================================
# CONFIGURAZIONE
# ==========================================

ssm = boto3.client('ssm', region_name='eu-north-1')

def get_parameter(name):
    response = ssm.get_parameter(Name=name, WithDecryption=True)
    return response['Parameter']['Value']

# Inizializzazione
AWS_REGION = get_parameter("REGION")
SQS_QUEUE_URL = get_parameter("SQS_QUEUE_URL")
S3_BUCKET = get_parameter("S3_BUCKET")
SUPABASE_URL = get_parameter("SUPABASE_URL")
SUPABASE_KEY = get_parameter("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
s3_client = boto3.client('s3', region_name=AWS_REGION)
sqs_client = boto3.client('sqs', region_name=AWS_REGION)

mediaconvert_client = boto3.client('mediaconvert', region_name="eu-north-1", endpoint_url="https://86huxbikc.mediaconvert.eu-north-1.amazonaws.com")

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

def process_generate_hls(video_id, stream_url):
    """Converte il video in HLS. Usa il remuxing ultra-veloce se il codec lo permette."""
    update_video_status(video_id, "CONVERTING")
    print(f"[{video_id}] Analisi file {stream_url}...")
    
    tmp_dir = f"/tmp/hls_{video_id}"
    os.makedirs(tmp_dir, exist_ok=True)
    
    try:
        index_file = os.path.join(tmp_dir, 'index.m3u8')
        
        # 1. Controlla i codec del file originale
        video_codec, audio_codec = check_media_codecs(stream_url)
        print(f"[{video_id}] Codec rilevati -> Video: {video_codec}, Audio: {audio_codec}")
        
        # 2. Costruisci il comando dinamicamente
        ffmpeg_cmd = [
            'ffmpeg', 
            '-i', stream_url,
            '-start_number', '0',
            '-hls_time', '10',
            '-hls_list_size', '0',
            '-f', 'hls'
        ]
        
        if video_codec == 'h264':
            print(f"[{video_id}] Video H.264 rilevato! Attivazione Remuxing Ultra-Veloce...")
            ffmpeg_cmd.extend(['-c:v', 'copy']) # Copia i pixel, zero sforzo CPU!
        else:
            print(f"[{video_id}] Codec {video_codec} rilevato: fallback su MediaConvert.")
            run_mediaconvert_job(video_id, s3_source)
            return  # Esci da questa funzione, MediaConvert gestirà tutto
            
        if audio_codec == 'aac':
            ffmpeg_cmd.extend(['-c:a', 'copy']) # L'audio è già perfetto, lo copio
        else:
            ffmpeg_cmd.extend(['-c:a', 'aac'])  # L'audio non è per il web, lo converto (velocissimo)
            
        ffmpeg_cmd.append(index_file)
        
        # 3. Lancia FFmpeg
        print(f"[{video_id}] Lancio FFmpeg in corso...")
        subprocess.run(ffmpeg_cmd, check=True)
        
        update_video_status(video_id, "CONVERTED")
        
        # 4. Carica su S3
        print(f"[{video_id}] Upload HLS su S3...")
        for filename in os.listdir(tmp_dir):
            local_path = os.path.join(tmp_dir, filename)
            s3_dest_key = f"{video_id}/hls/{filename}"
            print(f"[{video_id}] Uploading {filename} to S3 as {s3_dest_key}...")
            s3_client.upload_file(local_path, S3_BUCKET, s3_dest_key)
            
        print(f"[{video_id}] HLS completato e caricato!")
        update_video_status(video_id, "READY")

    finally:
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
            print(f"[{video_id}] Pulizia disco completata.")
            
def run_mediaconvert_job(video_id, s3_source):
    """Delega la transcodifica a MediaConvert se il codec non è standard."""
    print(f"[{video_id}] Codec non standard. Avvio MediaConvert...")
    
    output_s3_url = f"s3://{S3_BUCKET}/{video_id}/hls/"
    
    job_settings = {
        "Role": os.environ["MEDIACONVERT_ROLE_ARN"],
        "Settings": {
            "Inputs": [{"FileInput": f"s3://{S3_BUCKET}/{s3_source}"}],
            "OutputGroups": [{
                "Name": "Apple HLS",
                "OutputGroupSettings": {
                    "Type": "HLS_GROUP_SETTINGS",
                    "HlsGroupSettings": {"Destination": output_s3_url, "SegmentLength": 10}
                },
                "Outputs": [{
                    "ContainerSettings": {"Container": "M3U8"},
                    "VideoDescription": {"CodecSettings": {"Codec": "H_264", "H264Settings": {"RateControlMode": "QVBR"}}},
                    "AudioDescriptions": [{"CodecSettings": {"Codec": "AAC"}}]
                }]
            }]
        }
    }
    mediaconvert_client.create_job(**job_settings)
    # Nota: Non attendiamo il completamento qui per non bloccare il worker
    print(f"[{video_id}] Job MediaConvert inviato.")
    
def process_extract_clip(clip_id, s3_source, video_id):
    """Estrae una clip video frame-accurate partendo da un timestamp e la carica su S3."""
    print(f"[{clip_id}] Inizio estrazione clip del video {video_id}, presente al path {s3_source}...")
    
    try:
        # Query per recuperare i parametri necessari
        response = supabase.table("clips").select("title, anchor_time, offset_start, offset_end").eq("id", clip_id).single().execute()
        data = response.data
        
        anchor_time = data['anchor_time'] / 1000.0
        pre_buffer = data['offset_start']
        post_buffer = data['offset_end']
        title = data["title"]
        
        start_time = max(0, anchor_time - pre_buffer)
        duration = pre_buffer + post_buffer
        
        local_output = f"/tmp/clip_{clip_id}.mp4"
        
        stream_url = get_presigned_url(S3_BUCKET, s3_source)
        
        # Esecuzione estrazione
        ffmpeg_cmd = [
            'ffmpeg', '-ss', str(start_time), '-i', stream_url,
            '-t', str(duration), '-c:v', 'libx264', '-preset', 'veryfast',
            '-c:a', 'aac', '-y', local_output
        ]
        subprocess.run(ffmpeg_cmd, check=True)
        
        # Upload
        s3_dest_key = f"{video_id}/clips/{title}.mp4"
        s3_client.upload_file(local_output, S3_BUCKET, s3_dest_key)
        
        # Aggiornamento stato su Supabase
        supabase.table("clips").update({"status": "CLIPPED", "s3_source": s3_dest_keys}).eq("id", clip_id).execute()
        print(f"[{clip_id}] Clip estratta e caricata con successo.")
        
    except Exception as e:
        print(f"[{clip_id}] Errore durante l'estrazione: {e}")
        supabase.table("clips").update({"status": "ERROR"}).eq("id", clip_id).execute()
    finally:
        if os.path.exists(local_output):
            os.remove(local_output)


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
                
                if job_type == 'generate_hls':
                    stream_url = get_presigned_url(S3_BUCKET, body['s3_source'])
                    process_generate_hls(body['video_id'], stream_url)
                    
                elif job_type == 'extract_clip':
                    process_extract_clip(
                        clip_id=body['clip_id'],
                        s3_source=body['s3_source'],
                        video_id=body['video_id']
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