<!-- @format -->

- Pull RabbitMQ image from docker

  ## docker pull rabbitmq:management

- Run RabbitMQ image

  ## docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:management

- RabbitMQ UI
  - Link: http://localhost:15672/#/queues
  - username: guest
  - password: guest
