from django.urls import path

from . import views

urlpatterns = [
    path("fetch/", views.FetchSWApiView.as_view()),
    path("datasets/", views.DatasetView.as_view({"get": "list"})),
    path("datasets/<int:pk>", views.DatasetView.as_view({"get": "retrieve"})),
    path("datasets/<int:pk>/columns", views.ColumnsView.as_view()),
]
