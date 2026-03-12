# gemini_call/voice_webhook.py

from flask import request, url_for
from twilio.twiml.voice_response import VoiceResponse, Gather

from .gemini_service import get_gemini_response
from . import bp

@bp.route('/voice-webhook', methods=['POST'])
def voice_webhook():
    resp = VoiceResponse()

    speech_result = request.values.get('SpeechResult', '').strip()
    call_sid = request.values.get('CallSid', 'unknown')

    if not speech_result:
        # First call / greeting
        gather = Gather(
            input='speech',
            action=url_for('gemini_call.voice_webhook'),
            method='POST',
            timeout=5,
            speech_timeout='auto',
            language='en-IN',
            enhanced=True,                  # ← very important: uses premium model
            speech_model='phone_call',
        )

        gather.say(
            "Hello, am Alice from Xpress Health",
            voice="Google.en-IN-Neural2-A"
        )
        resp.append(gather)

        # Silence → repeat
        resp.say("ക്ഷമിക്കണം, ഞാൻ കേട്ടില്ല.")
        resp.pause(length=1)
        resp.redirect(url_for('gemini_call.voice_webhook'))

    else:
        # User spoke → Gemini
        try:
            gemini_reply = get_gemini_response(speech_result, call_sid)
        except Exception as e:
            print(f"Gemini error: {e}")
            gemini_reply = "ക്ഷമിക്കണം, എന്തോ പ്രശ്നമുണ്ടായി. ഒന്നുകൂടി പറയാമോ?"

        lower_reply = gemini_reply.lower()
        if any(word in lower_reply for word in ["goodbye", "bye", "thank you", "നന്ദി", "വീണ്ടും വിളിക്കാം"]):
            resp.say(gemini_reply, voice="Google.en-IN-Neural2-A")
            resp.hangup()                     # ← FIXED HERE
            return str(resp)

        # Continue listening
        gather = Gather(
            input='speech',
            action=url_for('gemini_call.voice_webhook'),
            method='POST',
            timeout=5,                        # ← increased from 4 → more natural
            speech_timeout='auto',
            language='en-IN',
        )
        gather.say(gemini_reply, voice="Google.en-IN-Neural2-A")
        resp.append(gather)

        # Silence fallback on subsequent turns
        resp.say("ക്ഷമിക്കണം, ഞാൻ കേട്ടില്ല... ഒന്നുകൂടി പറയാമോ?")
        resp.pause(length=1)
        resp.redirect(url_for('gemini_call.voice_webhook'))

    return str(resp)