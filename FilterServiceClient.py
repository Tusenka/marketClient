import grpc
import Content_pb2_grpc
import Content_pb2


class FilterClient(object):
    """
    Client for gRPC functionality
    """

    def __init__(self):
        self.host = 'localhost'
        self.server_port = 50051

        # instantiate a channel
        self.channel = grpc.insecure_channel(
            '{}:{}'.format(self.host, self.server_port))

        # bind the client and the server
        self.stub = Content_pb2_grpc.FilterServiceStub(self.channel)

    def get_url(self ):
        """
        Client function to call the rpc for GetServerResponse
        """
        filter = Content_pb2.Filter()
        filter_property = filter.filter_properties.add()
        filter_property.property.property = "Состав"
        filter_property.property.values[:] = ["дуванчик"]
        filter_property.predicate = "not"
        print(f'{filter}')
        return self.stub.GetContent(filter)


if __name__ == '__main__':
    client = FilterClient()
    result = client.get_url()
    print(f'{result}')