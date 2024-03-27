from cryptography.fernet import Fernet
import json

from .env_reader import env_reader


class EncyptionHandler:
    def __init__(self) -> None:
        envs = env_reader.get()
        if "SERVICE_SECRET_KEY" in envs and envs["SERVICE_SECRET_KEY"]:
            self.encyption_key = envs["SERVICE_SECRET_KEY"]
            self.fernet = Fernet(self.encyption_key)
            env_reader.remove("SERVICE_SECRET_KEY")
        del envs

    def decrypt(self, data: str) -> dict:
        try:
            message = self.fernet.decrypt(data.encode()).decode()
            return json.loads(message)
        except Exception as e:
            pass

    def encrypt(self, data: dict) -> str:
        message = json.dumps(data)
        return self.fernet.encrypt(message.encode()).decode()

    def custom_data_encrypt(self,encyption_key,  data: dict) -> str:
        _fernet = Fernet(encyption_key)
        message = json.dumps(data)
        return _fernet.encrypt(message.encode()).decode()

