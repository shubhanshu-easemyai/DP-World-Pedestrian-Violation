import os
import json


class EnvReader:
    def __init__(self) -> None:
        self.set()

    def set(self):
        self.envs = {}
        if os.path.exists(os.path.join(os.getcwd(), "envs.json")):
            with open(os.path.join(os.getcwd(), "envs.json"), "r") as f:
                data = json.load(f)
                for key, val in data.items():
                    os.environ[key] = str(val) if type(val) != dict else json.dumps(val)

        for key, val in os.environ.items():
            try:
                val = json.loads(val)
                if type(val) == str:
                    val = json.loads(val.replace("'", '"'))
            except Exception as e:
                pass
            if key.find("RDX") != -1:
                self.envs.update({key.split("RDX_")[-1]: val})
                del os.environ[key]

    def get(self):
        return self.envs

    def remove(self, env_name: str):
        if env_name in self.envs:
            self.envs.pop(env_name)
        return


env_reader = EnvReader()
