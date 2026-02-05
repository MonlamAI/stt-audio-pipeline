from pathlib import Path
import shutil

def collect_segments(prefix, source, destination_folder):
    """
    Collects audio segments from source folders starting with a given prefix
    and copies them to the destination folder.
    
    Args:
        prefix (str): The prefix to filter source folders.
        source (str): The path to the source folder.
        destination_folder (str): The path to the destination folder.
    """
    source = Path(source)
    destination_folder = Path(destination_folder)
    destination_folder.mkdir(parents=True, exist_ok=True)

    for source_folder in source.iterdir():
        if source_folder.is_dir() and source_folder.name.startswith(prefix):
            for wav_file in source_folder.glob("**/*.wav"):
                destination_path = destination_folder / wav_file.name
                shutil.copy2(wav_file, destination_path)
                print(f"Copied {wav_file} to {destination_path}")

    print("Copying complete.")

