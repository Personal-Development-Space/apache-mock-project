from django.db import models

# Create your models here.


class SalesByCardType(models.Model):
    # id = models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')
    batch_no = models.IntegerField()
    card_type = models.CharField(max_length=50)
    total_sales = models.FloatField()


class SalesByCountry(models.Model):
    # id = models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')
    batch_no = models.IntegerField()
    country = models.CharField(max_length=50)
    total_sales = models.FloatField()