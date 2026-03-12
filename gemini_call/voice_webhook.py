# gemini_call/voice_webhook.py

import os
import tempfile
import base64
from flask import request, url_for
from twilio.twiml.voice_response import VoiceResponse, Gather, Play
from elevenlabs.client import ElevenLabs
from elevenlabs import VoiceSettings

from .gemini_service import get_gemini_response
from . import bp

# ── ElevenLabs Configuration ────────────────────────────────────────────────
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
if not ELEVENLABS_API_KEY:
    raise ValueError("ELEVENLABS_API_KEY not set in environment variables")

eleven_client = ElevenLabs(api_key=ELEVENLABS_API_KEY)

# Choose your preferred voice – replace with actual Voice ID from ElevenLabs
# Examples: Indian-accented voices (copy from ElevenLabs dashboard)
ELEVEN_VOICE_ID = "WZlYpi1yf6zJhNWXih74"          # example: "Serena" or similar Indian English
ELEVEN_MODEL_ID = "eleven_flash_v2_5"              # fast & good quality
ELEVEN_OUTPUT_FORMAT = "ulaw_8000"                 # μ-law 8kHz – required by Twilio

def generate_elevenlabs_audio(text: str) -> str:
    """
    Generate μ-law 8kHz audio from ElevenLabs and return a public URL.
    
    For development: saves to /static/audio/ folder (must exist and be served)
    For production: upload to S3 / Cloudinary / Twilio Assets and return URL
    """
    try:
        audio_stream = eleven_client.text_to_speech.convert(
            voice_id=ELEVEN_VOICE_ID,
            optimize_streaming_latency=1,  # 0=best quality, 1-4=faster
            output_format=ELEVEN_OUTPUT_FORMAT,
            text=text,
            model_id=ELEVEN_MODEL_ID,
            voice_settings=VoiceSettings(
                stability=0.5,
                similarity_boost=0.75,
                style=0.0,
                use_speaker_boost=True,
            )
        )

        # For MVP: save to static folder (make sure 'static/audio' exists)
        filename = f"audio_{hash(text)}.ulaw"
        filepath = os.path.join("static", "audio", filename)

        with open(filepath, "wb") as f:
            for chunk in audio_stream:
                if chunk:
                    f.write(chunk)

        # Return URL that Flask can serve
        # Adjust domain/port in production (or use full https URL)
        return url_for('static', filename=f"audio/{filename}", _external=True)

    except Exception as e:
        print(f"ElevenLabs error: {e}")
        # Fallback text (very short) in case of failure
        return None  # we'll handle fallback in calling code

@bp.route('/voice-webhook', methods=['POST'])
def voice_webhook():
    resp = VoiceResponse()

    speech_result = request.values.get('SpeechResult', '').strip()
    call_sid = request.values.get('CallSid', 'unknown')

    if not speech_result:
        # ── First call / greeting ───────────────────────────────────────
        gather = Gather(
            input='speech',
            action=url_for('gemini_call.voice_webhook'),
            method='POST',
            timeout=5,
            speech_timeout='auto',
            language='en-IN',
            enhanced=True,
            speech_model='phone_call',
        )

        audio_url = generate_elevenlabs_audio(
            "Hello, Am alice from Xpress Health."
        )
        if audio_url:
            gather.play(audio_url)
        else:
            # Fallback if ElevenLabs fails
            gather.say(
                "Hello, Am alice from Xpress Health.",
                voice="Google.en-IN-Neural2-A"
            )

        resp.append(gather)

        # Silence → repeat
        fallback_url = generate_elevenlabs_audio("Sorry, I didn't hear you. Please try again.")
        if fallback_url:
            resp.play(fallback_url)
        else:
            resp.say("Sorry, I didn't hear you.", voice="Google.en-IN-Neural2-A")
        resp.pause(length=1)
        resp.redirect(url_for('gemini_call.voice_webhook'))

    else:
        # ── User spoke → Gemini ─────────────────────────────────────────
        try:
            gemini_reply = get_gemini_response(speech_result, call_sid)
        except Exception as e:
            print(f"Gemini error: {e}")
            gemini_reply = "Sorry, something went wrong. Could you please repeat?"

        lower_reply = gemini_reply.lower()
        if any(word in lower_reply for word in ["goodbye", "bye", "thank you", "നന്ദി", "വീണ്ടും വിളിക്കാം"]):
            audio_url = generate_elevenlabs_audio(gemini_reply)
            if audio_url:
                resp.play(audio_url)
            else:
                resp.say(gemini_reply, voice="Google.en-IN-Neural2-A")
            resp.hangup()
            return str(resp)

        # Continue listening
        gather = Gather(
            input='speech',
            action=url_for('gemini_call.voice_webhook'),
            method='POST',
            timeout=5,
            speech_timeout='auto',
            language='en-IN',
            enhanced=True,
            speech_model='phone_call',
        )

        audio_url = generate_elevenlabs_audio(gemini_reply)
        if audio_url:
            gather.play(audio_url)
        else:
            gather.say(gemini_reply, voice="Google.en-IN-Neural2-A")

        resp.append(gather)

        # Silence fallback on subsequent turns
        fallback_url = generate_elevenlabs_audio("Sorry, I didn't catch that. Could you say it again?")
        if fallback_url:
            resp.play(fallback_url)
        else:
            resp.say("Sorry, I didn't catch that.", voice="Google.en-IN-Neural2-A")
        resp.pause(length=1)
        resp.redirect(url_for('gemini_call.voice_webhook'))

    return str(resp)