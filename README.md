# th2 common library (Python)

## Installation
```
pip install th2-common
```
This package can be found on [PyPI](https://pypi.org/project/th2-common/ "th2 common library").

## Usage

First things first, you need to import `CommonFactory` class:
```
from th2_common.schema.factory.common_factory import CommonFactory
```
Then you create an instance of imported class, choosing one of the options:
1. Create factory with configs from default path (`/var/th2/config/*`):
    ```
    factory = CommonFactory()
    ```
2. Create factory with configs from specified directory path (`path/*`):
    ```
    factory = CommonFactory(config_path=...)
    ```
3. Create factory with configs from specified file paths:
    ```
    factory = CommonFactory(rabbit_mq_config_filepath=...,
                            mq_router_config_filepath=...,
                            grpc_router_config_filepath=...,
                            cradle_config_filepath=...,
                            custom_config_filepath=...)
    ```
4. Create factory with configs from command line arguments (`sys.argv`):
    ```
    factory = CommonFactory.create_from_arguments()
    ```
5. Create factory with configs from specified arguments:
    ```
    factory = CommonFactory.create_from_arguments(args)
    ```
    You can use one of the following groups of arguments. Arguments from different
    groups cannot be used together. 
    
    The first group:
    * --rabbitConfiguration - path to json file with RabbitMQ configuration
    * --messageRouterConfiguration - path to json file with configuration for MessageRouter
    * --grpcRouterConfiguration - path to json file with configuration for GrpcRouter
    * --cradleConfiguration - path to json file with configuration for Cradle
    * --customConfiguration - path to json file with custom configuration
    * --dictionariesDir - path to directory which contains files with encoded dictionaries
    * --prometheusConfiguration - path to json file with configuration for prometheus metrics server
    * --boxConfiguration - path to json file with boxes configuration and information
    * -c/--configs - folder with json files for schemas configurations with special names:
    1. rabbitMq.json - configuration for RabbitMQ
    2. mq.json - configuration for MessageRouter
    3. grpc.json - configuration for GrpcRouter
    4. cradle.json - configuration for cradle
    5. custom.json - custom configuration
    
    The second group:
    * --namespace - the namespace in Kubernetes to search config maps
    * --boxName - the name of the target th2 box placed in the specified Kubernetes namespace
    * --contextName - the context name to search connect parameters in Kube config
    
    Their usage is discovered further.
    
6. Create factory with a namespace in Kubernetes and the name of the target th2 box from Kubernetes:
    ```
    factory = CommonFactory.create_from_kubernetes(namespace, box_name)
    ```
    It also can be called by using `create_from_arguments(args)` with arguments `--namespace` and `--boxName`.
    
7. Create factory with a namespace in Kubernetes, the name of the target th2 box from Kubernetes and the name of context to choose the context from Kube config: 
    ```
    var factory = CommonFactory.create_from_kubernetes(namespace, boxName, contextName);
    ```
    It also can be called by using `create_from_arguments(args)` with arguments `--namespace`, `--boxName` and `--contextName`. 
    ContextName parameter is `None` by default; if it is set to `None`, the current context will not be changed.
### Requirements for creating factory with Kubernetes

1. It is necessary to have Kubernetes configuration written in ~/.kube/config. See more on kubectl configuration [here](https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/).

2. Also note that `generated_configs` directory will be created to store `.json` files with configs from Kubernetes. Those files are overridden when `CommonFactory.create_from_kubernetes(namespace, box_name)` is invoked again. 
3. User needs to have authentication with service account token that has necessary access to read CRs and secrets from the specified namespace.

After that you can get various `Routers` through `factory` properties:
```
message_parsed_batch_router = factory.message_parsed_batch_router
message_raw_batch_router = factory.message_raw_batch_router
event_batch_router = factory.event_batch_router
```

`message_parsed_batch_router` is working with `MessageBatch` <br>
`message_raw_batch_router` is working with `RawMessageBatch` <br>
`event_batch_router` is working with `EventBatch`

See [th2-grpc-common](https://github.com/th2-net/th2-grpc-common/blob/master/src/main/proto/th2_grpc_common/common.proto "common.proto") for details.

With `router` created, you can subscribe to pin (specifying callback function) or send data that router works with:
```
router.subscribe(callback)  # subscribe to only one pin 
router.subscribe_all(callback)  # subscribe to one or several pins
router.send(message)  # send to only one pin
router.send_all(message)  # send to one or several pins
```
You can do these actions with extra pin attributes in addition to default ones.
```
router.subscribe(callback, attrs...)  # subscribe to only one pin
router.subscribe_all(callback, attrs...)  # subscribe to one or several pins
router.send(message, attrs...)  # send to only one pin
router.send_all(message, attrs...)  # send to one or several pins
```
Default attributes are:
- `message_parsed_batch_router`
    - Subscribe: `subscribe`, `parsed`
    - Send: `publish`, `parsed`
- `message_raw_batch_router`
    - Subscribe: `subscribe`, `raw`
    - Send: `publish`, `raw`
- `event_batch_router`
    - Subscribe: `subscribe`, `event`
    - Send: `publish`, `event`
