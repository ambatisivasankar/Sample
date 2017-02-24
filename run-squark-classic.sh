#!/bin/bash

docker run -it --privileged --cap-add SYS_ADMIN --network=advana --rm squarkbox_2 $@
