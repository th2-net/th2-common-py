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

With `router` created, you can subscribe to pins (specifying callback function) or send data that router works with:
```
router.subscribe(callback)  # subscribe to only one pin 
router.subscribe_all(callback)  # subscribe to one or several pins
router.send(message)  # send to only one pim
router.send_all(message)  # send to one or several pins
```
You can do these actions provide pin attributes in addition to default ones.
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
