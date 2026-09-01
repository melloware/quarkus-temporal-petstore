<div align="center">
<img src="https://github.com/melloware/temporal-purchase-order/blob/main/docs/quarkus-petstore-logo.png" width="400" height="294" >
  
# Quarkus Temporal Petstore
</div>

[![Quarkus](https://img.shields.io/badge/quarkus-power-blue?logo=quarkus&style=for-the-badge)](https://github.com/quarkusio/quarkus)
[![License](https://img.shields.io/badge/License-Apache%202.0-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)
[![Static Badge for Temporal Code Exchange](https://img.shields.io/badge/Temporal-Code_Exchange_Featured-blue?style=for-the-badge&logo=temporal&labelColor=141414&color=444CE7)](https://temporal.io/code-exchange/quarkus-temporal) 

Quarkus Temporal Petstore is a comprehensive demonstration using Quarkus and Temporal. It simulates placing a new order on your Petstore website and fulfilling it using a microservice architecture.
This use case has been featured on [Temporal's Code Exchange](https://temporal.io/code-exchange/quarkus-temporal) catalog.

While the code is surprisingly simple, under the hood this is using:

- [Quarkus + Temporal Extension](https://github.com/quarkiverse/quarkus-temporal) for super subatomic microservices
- [Temporal](https://www.temporal.io/) for indestructible microservices and workflow orchestration
- [Mailpit](https://mailpit.axllent.org/) email & SMTP testing tool
- [PostgresSQL](https://www.postgresql.org/) the world's most advanced open source relational database
- [Grafana LGTM](https://github.com/grafana/docker-otel-lgtm) for distributed tracing, metrics, and logs using OpenTelemetry

## Overview

The following workflow is orchestrated across these microservices.

- **PurchaseOrder Gateway Service** receives a new order through a REST service
- **NotificationService:** Email is sent to the customer notifying them the order has been received and is processing
- **OrderService:** Order is persisted to the database
- **PaymentService:** Credit Card is verified as valid with enough funds
- **WarehouseService:** Warehouse checks if there is enough inventory to fulfill this order
- **ShipmentService:** Shipment service registers the shipment and creates a tracking number
- **OrderService:** Order is marked as complete in the database with updated information
- **NotificationService:** Email is sent to the customer notifying them order is on the way with tracking number

If anything in this process fails a "compensating transaction" must occur with the following steps:

- **PaymentService:** must reverse the payment or release the credit card hold
- **OrderService:** must update the database with the failure information
- **NotificationService:** Email is sent to the customer notifying them something went wrong and to call customer service with a reference number.

Temporal handles retrying and waiting for your services to come back up. So it will track the workflow until it is completed or issue a failure if the whole workflow is not completed in time (by default 24 hours).

## Microservices

The following microservices have been created to simulate this system:

| Service                | Description                                                                               | Port | Dev UI URL                                                         |
| ---------------------- | ----------------------------------------------------------------------------------------- | ---- | ------------------------------------------------------------------ |
| Purchase Order Gateway | Workflow gateway service that starts and manages the workflow when the order is received. | 8082 | [http://localhost:8082/q/dev-ui/](http://localhost:8082/q/dev-ui/) |
| Notification Service   | Email notifications to customers about order progress, completion, or cancellation.       | 8089 | [http://localhost:8089/q/dev-ui/](http://localhost:8089/q/dev-ui/) |
| Order Service          | Tracking the order in the database.                                                       | 8090 | [http://localhost:8090/q/dev-ui/](http://localhost:8090/q/dev-ui/) |
| Payment Service        | Validating the payment such as credit card.                                               | 8086 | [http://localhost:8086/q/dev-ui/](http://localhost:8086/q/dev-ui/) |
| Warehouse Service      | Validating inventory availability and decrementing inventory.                             | 8989 | [http://localhost:8989/q/dev-ui/](http://localhost:8989/q/dev-ui/) |
| Shipment Service       | Registering the order with a shipment provider and getting a tracking number.             | 8877 | [http://localhost:8877/q/dev-ui/](http://localhost:8877/q/dev-ui/) |

## Docker Compose

To run this demo you will need Docker or Podman to run the compose stack. To run it:

```shell
$ cd docker-compose
$ docker-compose up -d
```

The above will start the following services:

| Service    | Description                                                | Port / URL                                       |
| ---------- | ---------------------------------------------------------- | ------------------------------------------------ |
| Temporal   | Temporal server and UI                                     | [http://localhost:8080](http://localhost:8080)   |
| Mailpit    | SMTP Mail collector to view emails generated by the system | [http://localhost:8025](http://localhost:8025)   |
| pgAdmin    | Administrative UI for PostgreSQL                           | [http://localhost:16543](http://localhost:16543) |
| Grafana    | OpenTelemetry traces, metrics and logs UI (admin/admin)    | [http://localhost:3000](http://localhost:3000)   |
| PostgreSQL | Database for both the orders and for Temporal storage      | 5432                                             |

## Running Workflow

First, from the root of the project you must compile and build all dependencies using `mvn clean install`.

Next, you will need to have all 6 services running using `mvn quarkus:dev`, I recommend running them each in their own command prompt window for easy testing and startup/shutdown.

To start the workflow go to [Gateway Service DevUI](http://localhost:8082/q/dev-ui/) and find the OpenAPI card. There you can use the OpenAPI UI to call the REST Service with the following example payload (make changes as you like):

```js
{
  "creditCard": {
    "cardNumber": "4400123487650987",
    "cardHolderName": "Homer Simpson",
    "expiryDate": "12/25",
    "cvv": "372",
    "type": "VISA"
  },
  "customerEmail": "homer.simpson@springfield.gov",
  "products": [
    {
      "sku": "DOG-COLLAR-001",
      "quantity": 5,
      "price": 19.99
    }
  ]
}
```

You should see the new Workflow in the Temporal UI as well as both an Order Received and Order Completed emails in the Mailpit UI.

## Induce Failures

There are a few ways to induce failures in the workflow.

1. **PaymentService ERROR:** Put a `price` of greater than 1000
1. **PaymentService ERROR:** Put a `customerEmail` of `bad_customer@foo.com`
1. **WarehouseService ERROR:** Put a `quantity` of greater than 20 over many orders

These will fail the workflow and trigger the compensating transactions.

## User Interface

Navigate to [http://localhost:8082/](http://localhost:8082/) to view the user interface and submit an order.

[![Petstore UI](https://github.com/melloware/temporal-purchase-order/blob/main/docs/petstore-ui.png)](http://localhost:8082/)

## Infrastructure Diagram

[![Infrastructure Diagram](https://github.com/melloware/temporal-purchase-order/blob/main/docs/quarkus-remote-workflow.png)]()

## State Transition Diagram

[![State Transition Diagram](https://github.com/melloware/quarkus-temporal-petstore/blob/main/docs/state-transition.png)]()

