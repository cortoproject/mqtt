/* mqtt_Connector.c
 *
 * This file contains the implementation for the generated interface.
 *
 * Don't mess with the begin and end tags, since these will ensure that modified
 * code in interface functions isn't replaced when code is re-generated.
 */

#include "mqtt.h"

/* $header() */
#include "mosquitto.h"

static void mqtt_onMessage(
    struct mosquitto *client,
    void *data, 
    const struct mosquitto_message *msg) 
{
    CORTO_UNUSED(client);
    corto_id name;
    corto_object o = NULL;
    mqtt_Connector this = data;
    char *ptr = msg->topic;
    char *valueStr = strchr(msg->payload, '{');

    printf("mqtt: recv '%s' => '%s'\n", msg->topic, msg->payload);

    corto_object owner = corto_setOwner(this);

    /* Extract name from topic */
    do {
        strcpy(name, ptr + 1);
    } while((ptr = strchr(ptr + 1, '/')));

    printf("mqtt: received update for '%s'\n", name);

    if ((o = corto_resolve(corto_replicator(this)->mount, name))) {
        printf("mqtt: '%s' already exists, deserializing...\n", corto_nameof(o));
        if (corto_updateBegin(o)) {
            if (corto_fromStr(&o, valueStr)) {
                corto_error("mqtt: failed to deserialize for %s (%s)\n", name, msg->payload);
                return;
            }
            corto_updateEnd(o);
        }
    } else {
        corto_id buffer; strcpy(buffer, msg->payload);
        char *typeStr = strchr(buffer, '{');
        *typeStr = '\0';

        printf("mqtt: resolve type '%s'\n", buffer);
        corto_object type = corto_resolve(NULL, buffer);
        if (!type) {
            corto_error("type '%s' not found", buffer);
            return;
        }

        printf("mqtt: create new object '%s'\n", name);
        o = corto_declareChild(corto_replicator(this)->mount, name, type);
        if (!o) {
            corto_error("mqtt: failed to create object '%s'", name);
            return;
        }

        printf("mqtt: deserializing... '%s'\n", corto_nameof(o));
        if (corto_fromStr(&o, valueStr)) {
            corto_error("mqtt: failed to deserialize for %s (%s)", name, msg->payload);
            return;
        }

        printf("mqtt: define object\n");
        if (corto_define(o)) {
            corto_error("mqtt: failed to define '%s'", corto_nameof(o));
            return;
        }
    }
    printf("mqtt: recv: OK\n");

    corto_setOwner(owner);
}

static void mqtt_onPublish(
    struct mosquitto *client,
    void *data,
    int mid)
{
    CORTO_UNUSED(client);
    CORTO_UNUSED(data);
    CORTO_UNUSED(mid);  
}    

static void mqtt_onConnect(
    struct mosquitto *client,
    void *something,
    int rc) 
{
    if (rc != 0) {
        corto_seterr("mqtt: unable to connect");
    }
}

static void mqtt_onLog(struct mosquitto *mosq, void *obj, int level, const char *str)
{
    printf("%s\n", str);
}

static void mqtt_nameToPath(corto_object o, corto_id buffer) {
    corto_char *ptr = buffer, *bptr, ch;
    bptr = ptr;

    if (o) {
        corto_fullname(o, buffer);
        for (; (ch = *ptr); ptr++, bptr++) {
            if (ch == ':') {
                *bptr = '/';
                ptr++;
            } else {
                *bptr = ch;
            }
        }
    }

    *bptr = '\0';
}

/* $end */

/* ::mqtt::Connector::construct() */
corto_int16 _mqtt_Connector_construct(mqtt_Connector this) {
/* $begin(::mqtt::Connector::construct) */
    struct mosquitto *mosq = mosquitto_new(NULL, TRUE, this);
    corto_id host;
    corto_uint16 port;
    corto_id topic;

    /* Strip out port from hostname */
    strcpy(host, this->host);
    char *portptr = strchr(host, ':');
    if (!portptr) {
        port = 1883;
    } else {
        port = atoi(portptr + 1);
        *portptr = '\0';
    }

    if(!mosq){
        corto_error("mqtt: out of memory");
        goto error;
    }

    mosquitto_connect_callback_set(mosq, mqtt_onConnect);
    mosquitto_publish_callback_set(mosq, mqtt_onPublish);
    mosquitto_message_callback_set(mosq, mqtt_onMessage);
    if (this->debug) {
        mosquitto_log_callback_set(mosq, mqtt_onLog);
    }

    if (mosquitto_loop_start(mosq)) {
        corto_error("mqtt: failed to start network thread");
        goto error;
    }

    /* Setup delegates for listening to corto updates */
    corto_notifyActionSet(&corto_replicator(this)->onDeclare, this, corto_function(mqtt_Connector_onDeclare_o));
    corto_notifyActionSet(&corto_replicator(this)->onUpdate, this, corto_function(mqtt_Connector_onUpdate_o));
    corto_notifyActionSet(&corto_replicator(this)->onDelete, this, corto_function(mqtt_Connector_onDelete_o));
    corto_invokeActionSet(&corto_replicator(this)->onInvoke, this, corto_function(mqtt_Connector_onInvoke_o));

    this->client = (corto_word)mosq;

    if (mosquitto_connect(mosq, host, port, 60)) {
        corto_error("mqtt: unable to connect");
        goto error;
    }

    mqtt_nameToPath(corto_replicator(this)->mount, topic);
    strcat(topic, "/#");
    if (mosquitto_subscribe(mosq, 0, topic, 1)) {
        corto_error("mqtt: failed to subscribe for topic");
        goto error;
    }

    return corto_replicator_construct(this);
error:
    return -1;
/* $end */
}

/* ::mqtt::Connector::destruct() */
corto_void _mqtt_Connector_destruct(mqtt_Connector this) {
/* $begin(::mqtt::Connector::destruct) */

    mosquitto_loop_stop((struct mosquitto*)this->client, true);

/* $end */
}

/* ::mqtt::Connector::onDeclare(object observable) */
corto_void _mqtt_Connector_onDeclare(mqtt_Connector this, corto_object observable) {
/* $begin(::mqtt::Connector::onDeclare) */

    /* << Insert implementation >> */

/* $end */
}

/* ::mqtt::Connector::onDelete(object observable) */
corto_void _mqtt_Connector_onDelete(mqtt_Connector this, corto_object observable) {
/* $begin(::mqtt::Connector::onDelete) */

    /* << Insert implementation >> */

/* $end */
}

/* ::mqtt::Connector::onInvoke(object instance,function f,octetseq args) */
corto_void _mqtt_Connector_onInvoke(mqtt_Connector this, corto_object instance, corto_function f, corto_octetseq args) {
/* $begin(::mqtt::Connector::onInvoke) */

    /* << Insert implementation >> */

/* $end */
}

/* ::mqtt::Connector::onUpdate(object observable) */
corto_void _mqtt_Connector_onUpdate(mqtt_Connector this, corto_object observable) {
/* $begin(::mqtt::Connector::onUpdate) */
    int ret = 0;
    corto_id topic;
    corto_id type;
    corto_string payload = NULL;

    mqtt_nameToPath(observable, topic);
    corto_asprintf(&payload, "%s{%s}", corto_fullname(corto_typeof(observable), type), corto_str(observable, 0));

    if ((ret = mosquitto_publish((struct mosquitto*)this->client, NULL, topic, strlen(payload) + 1, payload, 1, FALSE))) {
        switch (ret) {
        case MOSQ_ERR_INVAL: corto_error("mqtt: publish failed: invalid input"); break;
        case MOSQ_ERR_NOMEM: corto_error("mqtt: publish failed: out of memory"); break;
        case MOSQ_ERR_NO_CONN: corto_error("mqtt: publish failed: not connected"); break;
        case MOSQ_ERR_PROTOCOL: corto_error("mqtt: publish failed: protocol error"); break;
        case MOSQ_ERR_PAYLOAD_SIZE: corto_error("mqtt: publish failed: message too large (%d)", strlen(payload)); break;
        default: corto_error("mqtt: publish error: unknown error"); break;
        }
    }

    corto_dealloc(payload);

/* $end */
}
