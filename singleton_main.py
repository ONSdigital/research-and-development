'''
Example of using a singleton class that initialises the boto3 client
This class will be:
    Accessible globally throughout the codebase
    Only one instance will exist, and this instance is used every time
'''
from src.utils.singleton_boto import SingletonBoto


def main():

    my_client1 = SingletonBoto.get_client()

    if not (my_client1 is None):
        print("Client created")

    my_client2 = SingletonBoto.get_client()

    if my_client1 is my_client2:
        print("same client")
    else:
        print("different clients")


if __name__ == "__main__":
    main()
