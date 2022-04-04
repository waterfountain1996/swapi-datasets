from django.db import models


class Dataset(models.Model):
    filename = models.CharField(max_length=256)
    timestamp = models.DateTimeField()
