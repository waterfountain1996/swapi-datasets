from rest_framework.views import APIView
from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework.response import Response

from .fetch import read_dataset, read_and_aggregate_columns
from .models import Dataset
from .pipeline import fetch_dataset
from .serializers import DatasetSerializer


class FetchSWApiView(APIView):
    def get(self, _):
        fetch_dataset.delay()
        return Response({"ok": True, "message": "Fetching dataset"})


class DatasetView(ReadOnlyModelViewSet):
    queryset = Dataset.objects.all().order_by('-timestamp')
    serializer_class = DatasetSerializer

    def retrieve(self, request, pk):
        """Load rows from dataset.

        Use `offset` URL parameter for pagination.
        """
        try:
            dataset = Dataset.objects.get(pk=pk)
        except:
            return Response({"error": "Dataset not found"}, 404)

        try:
            offset = int(request.GET["offset"])
        except:
            offset = 0

        return Response(read_dataset(
            filename=dataset.filename,
            skip=offset))


class ColumnsView(APIView):
    def get(self, request, pk):
        """Select and aggregate columns.

        Use `columns` URL parameter which value is a comma separated list
            of columns like: `homeworld,birth_year`
        """
        try:
            dataset = Dataset.objects.get(pk=pk)
        except:
            return Response({"error": "Dataset not found"}, 404)

        try:
            columns = request.GET["columns"].split(",")
        except:
            return Response({"error": "No columns specified"}, 400)

        return Response(read_and_aggregate_columns(
            filename=dataset.filename,
            columns=columns))
