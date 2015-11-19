/* $CORTO_GENERATED
 *
 * mqtt_Connector.c
 *
 * Only code written between the begin and end tags will be preserved
 * when the file is regenerated.
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

    corto_object owner = corto_setOwner(this);

    /* Extract name from topic */
    strcpy(name, ptr);
    while((ptr = strchr(ptr + 1, '/'))) {
        strcpy(name, ptr + 1);
    }

    if ((o = corto_resolve(corto_replicator(this)->mount, name))) {
        if ((corto_ownerof(o) == this) && !corto_updateBegin(o)) {
            if (corto_fromStr(&o, valueStr)) {
                corto_error("mqtt: failed to deserialize for %s: %s (%s)\n",
                    name,
                    corto_lasterr(),
                    msg->payload);
                return;
            }
            corto_updateEnd(o);
        }
    } else {
        corto_id buffer;
        char *typeStr = strchr(msg->payload, '{');
        memcpy(buffer, msg->payload, typeStr - (char*)msg->payload);
        buffer[typeStr - (char*)msg->payload] = '\0';

        corto_object type = corto_resolve(NULL, buffer);
        if (!type) {
            corto_error("type '%s' not found", buffer);
            return;
        }

        o = corto_declareChild(corto_replicator(this)->mount, name, type);
        if (!o) {
            corto_error("mqtt: failed to create object '%s'", name);
            return;
        }

        if (corto_fromStr(&o, valueStr)) {
            corto_error("mqtt: failed to deserialize for %s: %s (%s)",
                name,
                corto_lasterr(),
                msg->payload);
            return;
        }

        if (corto_define(o)) {
            corto_error("mqtt: failed to define '%s'", corto_nameof(o));
            return;
        }
    }

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
    void *data,
    int rc)
{
    mqtt_Connector this = data;
    if (rc != 0) {
        corto_error("mqtt: unable to connect");
    } else {
        corto_id topic;

        /* Subscribe to subtree of mountpoint */
        corto_fullname(corto_replicator(this)->mount, topic);
        if (*topic && strcmp(topic, "/")) {
            strcat(topic, "/#");
        } else {
            strcpy(topic, "#");
        }

        if (mosquitto_subscribe((struct mosquitto*)this->client, 0, topic, 1)) {
            corto_error("mqtt: failed to subscribe for topic");
        }
    }
}

static void mqtt_onLog(struct mosquitto *mosq, void *obj, int level, const char *str)
{
    printf("%s\n", str);
}

/* $end */

corto_int16 _mqtt_Connector_construct(mqtt_Connector this) {
/* $begin(corto/mqtt/Connector/construct) */
    struct mosquitto *mosq = mosquitto_new(NULL, TRUE, this);
    corto_id host;
    corto_uint16 port;

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

    return corto_replicator_construct(this);
error:
    return -1;
/* $end */
}

corto_void _mqtt_Connector_destruct(mqtt_Connector this) {
/* $begin(corto/mqtt/Connector/destruct) */

    mosquitto_disconnect((struct mosquitto*)this->client);
    mosquitto_loop_stop((struct mosquitto*)this->client, false);

/* $end */
}

corto_void _mqtt_Connector_onDeclare(mqtt_Connector this, corto_object observable) {
/* $begin(corto/mqtt/Connector/onDeclare) */

    /* << Insert implementation >> */

/* $end */
}

corto_void _mqtt_Connector_onDelete(mqtt_Connector this, corto_object observable) {
/* $begin(corto/mqtt/Connector/onDelete) */

    /* << Insert implementation >> */

/* $end */
}

corto_void _mqtt_Connector_onInvoke(mqtt_Connector this, corto_object instance, corto_function f, corto_octetseq args) {
/* $begin(corto/mqtt/Connector/onInvoke) */

    /* << Insert implementation >> */

/* $end */
}

corto_void _mqtt_Connector_onUpdate(mqtt_Connector this, corto_object observable) {
/* $begin(corto/mqtt/Connector/onUpdate) */

    int ret = 0;
    corto_id topic;
    corto_id typeName;
    corto_string payload = NULL, value = NULL;
    corto_int32 mid = 0;
    corto_type t = corto_typeof(observable);

    if (corto_parentof(t) == corto_lang_o) {
        strcpy(typeName, corto_nameof(t));
    } else {
        corto_fullname(t, typeName);
    }

    corto_fullname(observable, topic);
    value = corto_str(observable, 0);
    if (value[0] == '{') {
        corto_asprintf(&payload, "%s%s", typeName, value);
    } else {
        corto_asprintf(&payload, "%s{%s}", typeName, value);
    }
    corto_dealloc(value);

    if ((ret = mosquitto_publish((struct mosquitto*)this->client, &mid, topic + 1, strlen(payload) + 1, payload, 1, FALSE))) {
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
