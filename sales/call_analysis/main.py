from datetime import datetime
import sys, os

from dotenv import dotenv_values

env_path = os.path.dirname(os.path.realpath(__file__))+ "/.env"
config = dotenv_values(env_path)

MAIN_FOLDER = config["MAIN_FOLDER"]

import torch
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline



if __name__ == '__main__':
    if sys.argv[1] == 'transcribe_call':
        transcript_folder = MAIN_FOLDER + 'transcripts/'
        audio_folder = MAIN_FOLDER + 'audio_files/'

        model = "openai/whisper-large-v3"

        device = "mps"
        # torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32
        torch_dtype = torch.float16

        model_id = "openai/whisper-large-v3"

        model_load_start_time = datetime.now()

        model = AutoModelForSpeechSeq2Seq.from_pretrained(
            model_id,
            torch_dtype=torch_dtype,
            low_cpu_mem_usage=False,
            use_safetensors=True,
        )
        model.to(device)

        processor = AutoProcessor.from_pretrained(model_id)

        pipe = pipeline(
            "automatic-speech-recognition",
            model=model,
            tokenizer=processor.tokenizer,
            feature_extractor=processor.feature_extractor,
            max_new_tokens=448,
            chunk_length_s=15,
            batch_size=8,
            # return_timestamps=True,
            torch_dtype=torch_dtype,
            device=device,
        )

        audio_files = [audio_folder + x for x in os.listdir(audio_folder)]

        audio_files = [x for x in audio_files if 'careerly_call_01_04_2024' in x]

        for audio_file in audio_files:
            transcript_file = os.path.basename(audio_file).replace(".mp4", ".txt")
            transcript_file = transcript_folder + transcript_file

            result = pipe(audio_file)

            with open(transcript_file, "w") as f:
                f.write(result["text"])