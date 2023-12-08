import jwt
import datetime
import requests

class JWTHandler:
    def __init__(self, host, username, password ):
        self.username = username
        self.password = password
        self.host = host
        self.jwt = None
        self.leeway = datetime.timedelta(minutes=2)
  
    def is_expired(self):
        if self.jwt == None:
            return True
    
        decoded_jwt = jwt.decode(self.jwt, options={"verify_signature": False})
        exp = decoded_jwt["exp"]
        exp_datetime = datetime.datetime.fromtimestamp(exp)
        now = datetime.datetime.now()
        leeway_expiry = exp_datetime - self.leeway

        return now > leeway_expiry
    
    def generate_tokens(self):
        print("==========Extrica Token Call===========")

        url = "https://"+self.host+"/iam/security/signin"

        payload = {
        "email": self.username, 
        "password":self.password
        }

        response = requests.post(url, json=payload)

        if response.status_code == 200:
            data = response.json()
            self.jwt = data["accessToken"]
        else:
            print("Error getting tokens:", response.text)

    def get_token(self):
        if self.is_expired():
            self.generate_tokens()
        
        return self.jwt