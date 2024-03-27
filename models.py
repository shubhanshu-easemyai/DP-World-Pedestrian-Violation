from mongoengine import *
import datetime


class SourceInfo(Document):
    source_id = StringField(required=True, unique=True)
    source_type = StringField()
    source_subtype = StringField()
    source_url = StringField()
    source_location_name = StringField()
    source_glocation_coordinates = ListField(StringField())
    source_frame_width = IntField()
    source_frame_hight = IntField()
    source_owner = StringField()
    source_name = StringField(required=True, unique=True)
    source_tags = ListField(StringField())
    resolution = ListField(IntField())

    def payload(self):
        return {
            "source_id": self.source_id,
            "source_name": self.source_name,
            "source_type": self.source_type,
            "source_subtype": self.source_subtype,
            "resolution": self.resolution,
        }


class UserInfo(Document):
    user_id = StringField(required=True)
    username = StringField()
    user_type = StringField(default="owner")

    def payload(self):
        return {
            "user_id": self.user_id,
            "username": self.username
        }


class UsecaseParameters(Document):
    source_details = ReferenceField(SourceInfo)
    user_details = ReferenceField(UserInfo)
    settings = DictField()
    created = DateTimeField(default=datetime.datetime.utcnow)

class GeneralSettings(Document):
    output_name = StringField()
    user_details = ReferenceField(UserInfo)
    settings = DictField()
