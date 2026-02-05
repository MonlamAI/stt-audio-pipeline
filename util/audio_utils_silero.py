import os
import subprocess
import torch
import torchaudio
import librosa
import numpy as np
from pydub import AudioSegment
from tqdm import tqdm
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

# Global parameters for audio segmentation
upper_limit = 30  # Maximum segment duration in seconds (increased from 10)
lower_limit = 2   # Minimum segment duration in seconds (increased from 1)

# Silero VAD model loading
def load_vad_model():
    """
    Load the Silero VAD model from torch hub
    """
    # Force torch to use CPU operations to avoid device conflicts
    device = torch.device('cpu')
    torch.set_grad_enabled(False)
    
    # Load the model on CPU
    model, utils = torch.hub.load(
        repo_or_dir='snakers4/silero-vad',
        model='silero_vad',
        force_reload=True,
        onnx=False,  # Use PyTorch version
        verbose=False
    )
    
    model = model.to(device)
    
    # Unpacking utils
    (get_speech_timestamps, 
     save_audio, 
     read_audio, 
     VADIterator, 
     collect_chunks) = utils
    
    return model, get_speech_timestamps, save_audio, read_audio

def sec_to_millis(sec):
    """Convert seconds to milliseconds"""
    return sec * 1000

def millis_to_sec(millis):
    """Convert milliseconds to seconds"""
    return millis / 1000

def frame_to_sec(frame, sr):
    """Convert audio frames to seconds"""
    return frame / sr

def sec_to_frame(sec, sr):
    """Convert seconds to audio frames"""
    return int(sec * sr)

def convert_to_wav_inplace(input_file):
    """
    Convert audio file to PCM-encoded WAV format in place using ffmpeg.
    Args:
    input_file (str): path to full audio file
    """
    try:
        # Generate temporary file path
        temp_file = input_file.replace(".wav", "_temp.wav")
        
        # Run ffmpeg command
        subprocess.run(
            ["ffmpeg", "-y", "-i", input_file, "-ac", "1", "-ar", "16000", temp_file],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        
        # Replace the original file with the temporary file
        os.replace(temp_file, input_file)
        print(f"Converted {input_file} to PCM-encoded WAV format.")
    except subprocess.CalledProcessError as e:
        print(f"Error converting {input_file}: {e.stderr.decode() if hasattr(e, 'stderr') else str(e)}")
        raise

def save_segment(segment, folder, prefix, id, start_ms, end_ms, dept):
    """Save an audio segment to file with appropriate naming"""
    os.makedirs(f"../data/{dept}_after_split/{folder}", exist_ok=True)
    segment.export(
        f"../data/{dept}_after_split/{folder}/{prefix}_{id:04}_{int(start_ms)}_to_{int(end_ms)}.wav",
        format="wav",
        parameters=["-ac", "1", "-ar", "16000"],
    )

def delete_file(file):
    """Delete a file"""
    os.remove(file)

def split_audio(audio_file, output_folder, dept):
    """
    Splits the full audio file into segments based on:
    1. Voice Activity Detection using Silero VAD
    2. librosa split based on volume for segments longer than upper_limit
    3. Blind chop to fit the range of upper_limit to lower_limit

    Args:
        audio_file (str): path to full audio file
        output_folder (str): where to store the split segments
        dept (str): department name for organizing output folders
    """
    # Ensure the audio is in the right format
    convert_to_wav_inplace(audio_file)
    print(f"{audio_file} {output_folder}")
    
    # Load Silero VAD model
    model, get_speech_timestamps, save_audio, read_audio = load_vad_model()
    
    # Set device - use CPU for consistency
    device = torch.device('cpu')
    model = model.to(device)
    
    # Load audio file
    wav = read_audio(audio_file, sampling_rate=16000)
    # Ensure wav is on the same device as the model
    wav = wav.to(device)
    
    # Get speech timestamps using Silero VAD
    speech_timestamps = get_speech_timestamps(
        wav, 
        model, 
        threshold=0.3,       # Lower threshold to better detect low tone voices (default is 0.5)
        min_speech_duration_ms=2000,  # Increased from 1000ms to get longer segments (2 seconds)
        min_silence_duration_ms=800,  # Increased from 500ms (0.8 seconds) to avoid frequent cuts
        speech_pad_ms=300,    # Add padding to speech segments to avoid cutting off words
        return_seconds=True   # Return timestamps in seconds
    )
    
    # Load audio for processing
    original_audio_segment = AudioSegment.from_file(audio_file)
    original_audio_ndarray, sampling_rate = torchaudio.load(audio_file)
    original_audio_ndarray = original_audio_ndarray[0].numpy()  # Convert to numpy array
    
    counter = 1
    for speech_segment in speech_timestamps:
        start_time = speech_segment['start']
        end_time = speech_segment['end']
        segment_duration = end_time - start_time
        
        # Handle segments within our desired duration range
        if segment_duration >= lower_limit and segment_duration <= upper_limit:
            vad_segment = original_audio_segment[
                sec_to_millis(start_time) : sec_to_millis(end_time)
            ]
            save_segment(
                segment=vad_segment,
                folder=output_folder,
                prefix=output_folder,
                id=counter,
                start_ms=sec_to_millis(start_time),
                end_ms=sec_to_millis(end_time),
                dept=dept
            )
            print(
                f"{counter} {segment_duration:.2f} {sec_to_millis(start_time):.2f} {sec_to_millis(end_time):.2f} vad"
            )
            counter += 1
            
        # For segments longer than the upper limit, apply additional splitting
        elif segment_duration > upper_limit:
            # Get audio section for this segment
            segment_audio = original_audio_ndarray[
                sec_to_frame(start_time, sampling_rate) : sec_to_frame(end_time, sampling_rate)
            ]
            
            # Use librosa to find non-silent sections based on volume
            # Reduced top_db from 40 to 30 to better detect low tone voices
            non_mute_segment_splits = librosa.effects.split(segment_audio, top_db=30)
            
            for split_start, split_end in non_mute_segment_splits:
                # Convert frame positions back to seconds relative to the original audio
                abs_split_start = start_time + frame_to_sec(split_start, sampling_rate)
                abs_split_end = start_time + frame_to_sec(split_end, sampling_rate)
                split_duration = abs_split_end - abs_split_start
                
                # Handle segments within our desired duration range after librosa split
                if split_duration >= lower_limit and split_duration <= upper_limit:
                    segment_split = original_audio_segment[
                        sec_to_millis(abs_split_start) : sec_to_millis(abs_split_end)
                    ]
                    save_segment(
                        segment=segment_split,
                        folder=output_folder,
                        prefix=output_folder,
                        id=counter,
                        start_ms=sec_to_millis(abs_split_start),
                        end_ms=sec_to_millis(abs_split_end),
                        dept=dept
                    )
                    print(
                        f"{counter} {split_duration:.2f} {sec_to_millis(abs_split_start):.2f} {sec_to_millis(abs_split_end):.2f} split"
                    )
                    counter += 1
                    
                # For segments still longer than the upper limit after librosa split, do blind chopping
                elif split_duration > upper_limit:
                    chop_length = split_duration / 2
                    while chop_length > upper_limit:
                        chop_length = chop_length / 2
                        
                    for j in range(int(split_duration / chop_length)):
                        chop_start = abs_split_start + (chop_length * j)
                        chop_end = abs_split_start + (chop_length * (j + 1))
                        
                        segment_split_chop = original_audio_segment[
                            sec_to_millis(chop_start) : sec_to_millis(chop_end)
                        ]
                        save_segment(
                            segment=segment_split_chop,
                            folder=output_folder,
                            prefix=output_folder,
                            id=counter,
                            start_ms=sec_to_millis(chop_start),
                            end_ms=sec_to_millis(chop_end),
                            dept=dept
                        )
                        print(
                            f"{counter} {chop_length:.2f} {sec_to_millis(chop_start):.2f} {sec_to_millis(chop_end):.2f} chop"
                        )
                        counter += 1

def split_audio_files(prefix, ext, audio_dir, dept):
    """
    Process and split all audio files with a specific prefix and extension.

    Args:
        prefix (str): Prefix for the filenames.
        ext (str): File extension of the audio files.
        audio_dir (str): Directory containing the audio files
        dept (str): Department name for organizing output folders
    """
    stt_files = [
        filename
        for filename in os.listdir(audio_dir)
        if filename.startswith(prefix)
        and os.path.isfile(os.path.join(audio_dir, filename))
    ]
    
    for stt_file in tqdm(stt_files):
        stt_file = stt_file.split(".")[0]
        split_audio(audio_file=f"{audio_dir}/{stt_file}.{ext}", output_folder=stt_file, dept=dept)

def convert_mp3_to_wav(mp3_file, output_folder):
    """
    Convert a single MP3 file to WAV format
    
    Args:
        mp3_file (str): Path to the MP3 file
        output_folder (str): Folder to save the WAV file
    """
    # Ensure output folder exists, create if necessary
    os.makedirs(output_folder, exist_ok=True)
    
    # Load MP3 file
    audio = AudioSegment.from_mp3(mp3_file)
    
    # Define output file path (replace .mp3 with .wav)
    wav_file = os.path.join(output_folder, os.path.splitext(os.path.basename(mp3_file))[0] + ".wav")
    
    # Export audio to WAV format
    audio.export(wav_file, format="wav")
    
    print(f"Converted {mp3_file} to {wav_file}")

def batch_convert_mp3_to_wav(input_dir, output_dir):
    """
    Convert all MP3 files in a directory to WAV format
    
    Args:
        input_dir (str): Directory containing MP3 files
        output_dir (str): Directory to save the WAV files
    """
    # Ensure output directory exists, create if necessary
    os.makedirs(output_dir, exist_ok=True)
    
    # List all files in the input directory
    files = os.listdir(input_dir)
    
    # Filter only .mp3 files
    mp3_files = [f for f in files if f.endswith(".mp3")]
    
    print(mp3_files)
    # Iterate over each MP3 file and convert to WAV
    for mp3_file in mp3_files:
        mp3_path = os.path.join(input_dir, mp3_file)
        convert_mp3_to_wav(mp3_path, output_dir)

def convert_to_16K(input_file, output_file):
    """
    Convert an audio file to 16kHz mono WAV format
    
    Args:
        input_file (str): Path to the input audio file
        output_file (str): Path to save the converted file
    
    Returns:
        bool: True if conversion was successful, False otherwise
    """
    # Check if the output file already exists
    if os.path.exists(output_file):
        print(f"Output file {output_file} already exists. Skipping conversion.")
        return False
    
    # Construct the ffmpeg command
    ffmpeg_command = [
        "ffmpeg",
        "-i", input_file,
        "-f", "wav",            # Force input format as WAV
        "-bitexact",            # Preserve exact precision
        "-acodec", "pcm_s16le", # Audio codec: PCM signed 16-bit little-endian
        "-ac", "1",             # Mono channel
        "-ar", "16000",         # 16 kHz sample rate
        output_file,
        "-y"                    # Overwrite output file if it exists
    ]
    
    # Join the ffmpeg command into a single string for subprocess
    ffmpeg_command_str = " ".join(ffmpeg_command)
    
    try:
        # Run ffmpeg command using subprocess
        subprocess.run(ffmpeg_command_str, shell=True, check=True, capture_output=True)
        print(f"Conversion successful: {input_file} -> {output_file}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Conversion failed: {e}")
        return False

def convert_all_to_16K(input_directory, output_directory_16K):
    """
    Convert all WAV files in a directory to 16kHz mono WAV format
    
    Args:
        input_directory (str): Directory containing WAV files
        output_directory_16K (str): Directory to save the converted files
    """
    if not os.path.exists(output_directory_16K):
        os.makedirs(output_directory_16K)

    for filename in os.listdir(input_directory):
        if filename.endswith(".wav"):
            input_file = os.path.join(input_directory, filename)
            output_file = os.path.join(output_directory_16K, filename)
            convert_to_16K(input_file, output_file)

def extract_audio(video_file, audio_file):
    """
    Extract audio from a video file
    
    Args:
        video_file (str): Path to the video file
        audio_file (str): Path to save the extracted audio
    """
    command = [
        "ffmpeg",
        "-i", video_file,
        "-q:a", "0",
        "-map", "a",
        audio_file
    ]
    try:
        subprocess.run(command, check=True)
        print(f"Extracted audio to {audio_file}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to extract audio from {video_file}. Error: {e}")
