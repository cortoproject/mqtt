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

    /* If the payload has been serialized as a corto string, the typename is
     * potentially prefixed to the value */
    char *valueStr = strchr(msg->payload, '{');

    /* mqtt is the owner of this thread. This ensures that all subsequent create
     * update / delete actions are performed with the right owner. Ownership
     * ensures to not trigger on own updates, and to only forward data from
     * other connectors (or the application). */
    corto_object prevOwner = corto_setOwner(this);

    /* Remove topic from name, so that name is relative to mount point. */
    name = msg->topic;
    if (this->topic) {
        name += strlen(this->topic) + 1;
    }

    corto_debug("mqtt: %s: received '%s'", msg->topic, msg->payload);

    /* Check if object already exists in object store */
    if ((o = corto_lookup(corto_mount(this)->mount, name))) {
        corto_debug("mqtt: found '%s' for '%s'", corto_fullpath(NULL, o), name);

        /* Only continue updating object when it is owned by mqtt */
        if (corto_owned(o)) {
            /* Start updating object (takes a writelock) */
            if (!corto_updateBegin(o)) {
                /* Serialize value from JSON string */
                if (corto_fromcontent(o, "text/json", valueStr)) {
                    corto_error("mqtt: failed to deserialize for %s: %s (%s)\n",
                        name,
                        corto_lasterr(),
                        msg->payload);

                    /* If deserialization fails, cancel the update. No notification
                     * will be sent. */
                    corto_updateCancel(o);
                    goto error;
                }
                /* Successful update. Send notification and unlock object */
                corto_updateEnd(o);
            } else {
                /* For some reason, couldn't start updating object */
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

        /* If the mount has been configured with a fixed type, use that type to
         * create a new object. Otherwise, look for type in payload. */
        if (corto_mount(this)->type) {
            strcpy(buffer, corto_mount(this)->type);
        } else {
            char *typeStr = strchr(msg->payload, '{');
            memcpy(buffer, msg->payload, typeStr - (char*)msg->payload);
            buffer[typeStr - (char*)msg->payload] = '\0';
        }

        /* Resolve type. If type wasn't yet loaded in corto, corto_resolve will
         * do a lookup on the package repository. If it doesn't exist there
         * either throw an error. Currently, the MQTT connector does not align
         * types. */
        corto_type type = corto_resolve(NULL, buffer);
        if (!type) {
            corto_error("mqtt: type '%s' not found", buffer);
            goto error;
        }

        corto_debug("mqtt: creating '%s' with type '%s'", name, buffer);

        /* Create a new object under the mountpoint. The name is derived from
         * the MQTT topic name. */
        o = corto_declareChild(corto_mount(this)->mount, name, type);
        if (!o) {
            corto_error("mqtt: failed to create object '%s'", name);
            goto error;
        }

        /* Serialize value from JSON payload */
        if (corto_fromcontent(o, "text/json", valueStr)) {
            corto_error("mqtt: failed to deserialize for %s: %s (%s)",
              name,
              corto_lasterr(),
              msg->payload);
            goto error;
        }

        /* Define object. This marks the point in time where the object value has
         * become ready for distribution. */
        if (corto_define(o)) {
            corto_error("mqtt: failed to define '%s'", corto_idof(o));
            goto error;
        }
    }

error:
    /* Restore previous owner */
    corto_setOwner(prevOwner);
}

static void mqtt_onConnect(
    struct mosquitto *client,
    void *data,
    int rc)
{
    /* Subscribe to topic when connected to the broker */
    if (rc != 0) {
        corto_error("mqtt: unable to connect to %s", this->host);
    } else {
        mqtt_Connector this = data;
        corto_id topic;
        strcpy(topic, this->topic);

        corto_ok("mqtt: connected to %s", this->host);

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

    /* onConnect subscribes for the topic. onMessage inserts data from MQTT into
     * corto. onLog traces debug information from Mosquitto. */
    mosquitto_connect_callback_set(mosq, mqtt_onConnect);
    mosquitto_message_callback_set(mosq, mqtt_onMessage);
    mosquitto_log_callback_set(mosq, mqtt_onLog);

    /* Start Mosquitto network thread */
    if (mosquitto_loop_start(mosq)) {
        corto_seterr("mqtt: failed to start network thread");
        goto error;
    }

    /* Register mosquitto object with corto object. Using the MQTT_KEY_CLIENT
     * key, subsequent functions can obtain the mosquitto object by calling
     * corto_olsGet(this, MQTT_KEY_CLIENT) */
    corto_olsSet(this, MQTT_KEY_CLIENT, mosq);

    /* Finally, connect to the broker. */
    if (mosquitto_connect(mosq, host, port, 60)) {
        corto_seterr("mqtt: unable to connect");
        goto error;
    }

    /* Invoke constructor of the mount. This will subscribe the mount for any
     * object under the mountpoint, so that the onUpdate method will be called
     * when notifications are sent. */
    return corto_mount_construct(this);
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

    /* Get object name relative to mount, prefix it with the topic */
    corto_path(objName, corto_mount(this)->mount, observable, "/");
    sprintf(topic, "%s/%s", this->topic, objName);

    /* Get object JSON */
    value = corto_contentof(NULL, "text/json", observable);

    /* If the type is not explicitly set, this connector will potentially
     * receive updates from objects of any type. In that case, prefix the type
     * to the payload so that a receiving application is able to figure out the
     * type of the object.
     * Note that this requires both sending and receiving side to use the same
     * type for the specified topic. */
    if (!corto_mount(this)->type) {
        corto_fullpath(typeName, corto_typeof(observable));

        /* If object is a primitive, wrap it in {} */
        if (value[0] == '{') {
            corto_asprintf(&payload, "%s%s", typeName, value);
        } else {
            corto_asprintf(&payload, "%s{%s}", typeName, value);
        }
    } else {
        payload = value;
    }

    /* Finally, publish the message to mqtt */
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
