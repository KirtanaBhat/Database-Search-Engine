from django.db import models

# Create your models here.
class Users(models.Model):
    name = models.CharField(max_length=100)
    employee_id = models.CharField(max_length=100)    
    username = models.CharField(max_length=100)
    password = models.CharField(max_length=100)

    def __str__(self):
        return self.name