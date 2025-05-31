# Leveraging OpenFlow for Bronze Data Ingestion

We will be levering OpenFlow to ingest data from Kafka. Data is coming from the
[NetworkRail](https://www.networkrail.co.uk/who-we-are/transparency-and-ethics/transparency/open-data-feeds/) Open Data Feeds.

The target flow we will build will look like this:

![img](./kafka_hol_overview.png)

This flow consumes the data from Kafka and then routes to different tables depending on the data.

Let's walk on the steps to get this flow built in openflow.

First let's navigate to the openflow canvas, and create a process group. Let's create it with a given name and remember to setup a parameter context. We will use that later.

![drag_process_group](./drag_process_group.gif)

We will get inside the process group. Just double click to get inside, and we will start by adding a
[ConsumeKafka](https://docs.snowflake.com/user-guide/data-integration/openflow/processors/consumekafka)
component.

To add it just drag the component into the canvas.

![add_consume_kafka](./add_consume_kafka.gif)

Before we use this component we need to configure it. So there are some settings to setup. Right click the component and select **Configure**:

![consume_kafka_props](./consume_kafka_props.png)

We need to setup:

* Kafka Connection Service
* Group ID set this to your given group_id
* Topics set this to **trust**

Setting up the "Kafka Connection Service"

We will use the "three-dot" menu. Click on it, and select the "Create new service" option

![consume_kafka_props_kafka_connection_service](./consume_kafka_props_kafka_connection_service.png)

This will open a dialog for you to select the service. Look for the

[Kafka3ConnectionService](https://docs.snowflake.com/en/user-guide/data-integration/openflow/controllers/kafka3connectionservice)


![kafka_connection_service_dialog](./kafka_connection_service_dialog.png)

After adding it we need to configure the service, click on the three dots and select go to service:

![kafka_connection_service_edit](./kafka_connection_service_edit.png)

Use the "three dot" menu again.

![kafka_connection_service_props](./kafka_connection_service_props.png)

Set up the bootstrap server with the BROKER url.

It is a good practice to verify your settings. Click on the check button and follow the dialogs.

![verify_kafka_connection_service](./verify_kafka_connection_service.png)

If everything is fine it will look like this:

![kafka_service_verified](./kafka_service_verified.png)

Before we continue, let's test that everything is fine. To do that lets do the following, we will drag a log attribute and we will connect our ConsumeKafka component to it. And then we will right click and select Start

![logging_test](./logging_test.png)

After a few seconds you should see that the queue starts to grow

![logging_test_2](./logging_test_2.png)\

Right click and select List queue

![logging_test_3](./logging_test_3.png)

You will be taken to an screen like this:
![image-20250528214404921](/Users/mrojas/Library/Application Support/typora-user-images/image-20250528214404921.png)

You can use this screen to see the "flow files" and their properties. You can also examine the flow files contents. In you select view content and set the view to json , you can view your message like this:

![logging_test_5](./logging_test_5.png)

Now let's stop our ConsumeKafka again and remove the LogAttribute and resume our flow.

And let continue with the rest of our Lab.

So the next thing we want to do is to be able to write to different tables depending on the Kafka message.

We will drag a RouteOnContent component to our canvas. And will select Configure as before. In the configure dialog we will use the + button to add new properties:

![route_on_content_props](./route_on_content_props.png)

And we will add three properties:

| property name | Value                   |
| ------------- | ----------------------- |
| is_msg_0001   | "msg_type"\s*:\s*"0001" |
| is_msg_0002   | "msg_type"\s*:\s*"0002" |
| is_msg_0003   | "msg_type"\s*:\s*"0003" |

![match_properties](./match_properties.png)

Also remember to set the Match Requirement

![match_requirement](./match_requirement.png)

We will use in this case some regular expressions that will allows to know which messages have a particular message type.

Ok we are done with the routing.

Now we need to send these flow files to our target tables: MOVEMENTS_RAW_0001, MOVEMENTS_RAW_0002, MOVEMENTS_RAW_0003

Let's drag a new processor into our canvas. Now we will be adding a PutSnowpipeStreaming processor.

This component can be used to send data into tables in snowflake. We will need to configure:

* Connection settings: We need an user/private key/account/database/schema/table_name

  * For the private key we will need to setup a [private key service](https://docs.snowflake.com/en/user-guide/data-integration/openflow/controllers/standardprivatekeyservice)
* Record Reader

  * We need a [JsonTreeReader](https://docs.snowflake.com/en/user-guide/data-integration/openflow/controllers/jsontreereader)

# Setting up the private key service.

Just as before, we can use the "three dot" menu to create a service (select it looking for StandardPrivateKeyService) and then configure it.

Before that we need to upload our private key.

To do that we will go to the parameter context and select the one for our processor group. We will then add a parameter with a name according to our process group, check the reference assets checkbox, use the upload button to upload our private key, and save it.

![setting_up_the_private_key_parameter](./setting_up_the_private_key_parameter.gif)

We can now go to configure our Private Key Service

![private_key_reference](./private_key_reference.png)

# Setting up the JsonTreeReader

For this service we just need to create a new one. Default settings will be enough for us.

Now we are ready to connect our tables. Drag from the RouteOnContent component to the PutSnowpipeComponent. You will see that a dialog with the properties we defined previously appears:

![connect_to_write](./connect_to_write.png)

And you need to setup the relationships for the write node.

![image-20250528223958763](./image-20250528223958763.png)

Now you are done for `MOVEMENTS_RAW_0001`. Just copy paste the now two times modify the processor name and table for `MOVEMENTS_RAW_0002` and `MOVEMENTS_RAW_0003` and drag the connections for `is_msg_0002` and `is_msg_0003`

And set the terminate relationship  for `RouteOnContent`

![route_on_content_terminate](./route_on_content_terminate.png)

With that your flow should look like this:

![kafka_hol_overview](./kafka_hol_overview.png)

And you can just start all nodes. You can go to your account and validate that data is flowing in the raw tables.
