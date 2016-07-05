/* $CORTO_GENERATED
 *
 * Connector.c
 *
 * Only code written between the begin and end tags will be preserved
 * when the file is regenerated.
 */

#include <corto/mqtt/mqtt.h>

/* $header() */
#include "mosquitto.h"

extern corto_uint8 MQTT_KEY_CLIENT;

static void mqtt_onMessage(
    struct mosquitto *client,
    void *data,
    const struct mosquitto_message *msg)
{
    char *name;
    corto_object o = NULL;
    mqtt_Connector this = data;
    char *valueStr = strchr(msg->payload, '{');

    corto_object prevOwner = corto_setOwner(this);

    name = msg->topic;
    if (this->topic) {
        name += strlen(this->topic) + 1;
    }

    corto_debug("mqtt: %s: received '%s'", msg->topic, msg->payload);

    if ((o = corto_lookup(corto_mount(this)->mount, name))) {
        corto_debug("mqtt: found '%s' for '%s'", corto_fullpath(NULL, o), name);
        if (corto_owned(o)) {
            if (!corto_updateBegin(o)) {
                if (corto_fromcontent(o, "text/json", valueStr)) {
                    corto_error("mqtt: failed to deserialize for %s: %s (%s)\n",
                        name,
                        corto_lasterr(),
                        msg->payload);
                    corto_updateCancel(o);
                    goto error;
                }
                corto_updateEnd(o);
            } else {
                corto_error("mqtt: failed to update '%s': %s", name, corto_lasterr());
                goto error;
            }
        } else {
            corto_debug("mqtt: '%s' not owned by me (%s, defined = %d), ignoring",
                corto_fullpath(NULL, o),
                corto_ownerof(o) ? corto_fullpath(NULL, o) : "local",
                corto_checkState(o, CORTO_DEFINED));
        }
    } else {
        corto_id buffer;
        corto_debug("mqtt: creating new object for '%s'", name);
        if (corto_mount(this)->type) {
            strcpy(buffer, corto_mount(this)->type);
        } else {
            char *typeStr = strchr(msg->payload, '{');
            memcpy(buffer, msg->payload, typeStr - (char*)msg->payload);
            buffer[typeStr - (char*)msg->payload] = '\0';
        }

        corto_type type = corto_resolve(NULL, buffer);
        if (!type) {
            corto_error("mqtt: type '%s' not found", buffer);
            goto error;
        }

        corto_debug("mqtt: creating '%s' with type '%s'", name, buffer);

        o = corto_declareChild(corto_mount(this)->mount, name, type);
        if (!o) {
            corto_error("mqtt: failed to create object '%s'", name);
            goto error;
        }

        if (corto_fromcontent(o, "text/json", valueStr)) {
            corto_error("mqtt: failed to deserialize for %s: %s (%s)",
              name,
              corto_lasterr(),
              msg->payload);
            goto error;
        }

        if (corto_define(o)) {
            corto_error("mqtt: failed to define '%s'", corto_idof(o));
            goto error;
        }
    }

error:
    corto_setOwner(prevOwner);
}

static void mqtt_onPublish(
    struct mosquitto *client,
    void *data,
    int mid)
{
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

        corto_ok("mqtt: connected to %s", this->host);

        strcpy(topic, this->topic);

        /* Subscribe to subtree of mountpoint */
        if (*topic && strcmp(topic, "/")) {
            strcat(topic, "/#");
        } else {
            strcpy(topic, "#");
        }

        corto_trace("mqtt: subscribing to %s", topic);

        if (mosquitto_subscribe(client, 0, topic, 1)) {
            corto_error("mqtt: failed to subscribe for topic");
        }

        corto_ok("mqtt: subscribed to %s", topic);
    }
}

static void mqtt_onLog(struct mosquitto *mosq, void *obj, int level, const char *str)
{
    corto_debug("%s", str);
}

/* $end */

corto_int16 _mqtt_Connector_construct(
    mqtt_Connector this)
{
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
        corto_seterr("mqtt: out of memory");
        goto error;
    }

    mosquitto_connect_callback_set(mosq, mqtt_onConnect);
    mosquitto_publish_callback_set(mosq, mqtt_onPublish);
    mosquitto_message_callback_set(mosq, mqtt_onMessage);
    mosquitto_log_callback_set(mosq, mqtt_onLog);

    if (mosquitto_loop_start(mosq)) {
        corto_seterr("mqtt: failed to start network thread");
        goto error;
    }

    corto_olsSet(this, MQTT_KEY_CLIENT, mosq);

    if (mosquitto_connect(mosq, host, port, 60)) {
        corto_seterr("mqtt: unable to connect");
        goto error;
    }

     corto_mount_construct(this);

     return 0;
error:
    return -1;
/* $end */
}

corto_void _mqtt_Connector_destruct(
    mqtt_Connector this)
{
/* $begin(corto/mqtt/Connector/destruct) */
    struct mosquitto *mosq = corto_olsGet(this, MQTT_KEY_CLIENT);

    corto_trace("mqtt: disconnecting...");

    mosquitto_disconnect(mosq);
    mosquitto_loop_stop(mosq, false);

/* $end */
}

corto_void _mqtt_Connector_onDeclare(
    mqtt_Connector this,
    corto_object observable)
{
/* $begin(corto/mqtt/Connector/onDeclare) */

    /* << Insert implementation >> */

/* $end */
}

corto_void _mqtt_Connector_onDelete(
    mqtt_Connector this,
    corto_object observable)
{
/* $begin(corto/mqtt/Connector/onDelete) */

    /* << Insert implementation >> */

/* $end */
}

corto_void _mqtt_Connector_onUpdate(
    mqtt_Connector this,
    corto_object observable)
{
/* $begin(corto/mqtt/Connector/onUpdate) */
    int ret = 0;
    corto_id topic, objName;
    corto_id typeName;
    corto_string payload = NULL, value = NULL;
    corto_int32 mid = 0;
    struct mosquitto *mosq = corto_olsGet(this, MQTT_KEY_CLIENT);
    if (!mosq) {
        corto_error("mqtt: no client registered for connector %s (%d)", corto_idof(this), MQTT_KEY_CLIENT);
        goto error;
    }

    /* Get object name relative to mount, prefix it with the topic */
    corto_path(objName, corto_mount(this)->mount, observable, "/");
    sprintf(topic, "%s/%s", this->topic, objName);

    /* Get object JSON */
    value = corto_contentof(NULL, "text/json", observable);

    if (!corto_mount(this)->type) {
        /* If connector handles multiple types, prefix data with type */
        corto_fullpath(typeName, corto_typeof(observable));

        if (value[0] == '{') {
            corto_asprintf(&payload, "%s%s", typeName, value);
        } else {
            corto_asprintf(&payload, "%s{%s}", typeName, value);
        }
    } else {
        payload = value;
    }

    if ((ret = mosquitto_publish(mosq, &mid, topic, strlen(payload) + 1, payload, 1, FALSE))) {
        switch (ret) {
        case MOSQ_ERR_INVAL: corto_error("mqtt: publish failed: invalid input"); break;
        case MOSQ_ERR_NOMEM: corto_error("mqtt: publish failed: out of memory"); break;
        case MOSQ_ERR_NO_CONN: corto_warning("mqtt: publish failed: not (yet) connected"); break;
        case MOSQ_ERR_PROTOCOL: corto_error("mqtt: publish failed: protocol error"); break;
        case MOSQ_ERR_PAYLOAD_SIZE: corto_error("mqtt: publish failed: message too large (%d)", strlen(payload)); break;
        default: corto_error("mqtt: publish error: unknown error"); break;
        }
    }

    corto_debug("mqtt: topic:%s payload:'%s'", topic, payload);

    if (payload != value) corto_dealloc(payload);

error:
    return;
/* $end */
}
