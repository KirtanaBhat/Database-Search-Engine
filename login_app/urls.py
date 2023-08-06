
from django.urls import path
from . import views


urlpatterns = [
    path('', views.loginPage, name='login'),
    
    # path('failed', views.login_failed, name='failed'),
]
 