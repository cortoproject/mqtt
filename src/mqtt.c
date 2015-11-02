/* $CORTO_GENERATED
 *
 * mqtt.c
 *
 * Only code written between the begin and end tags will be preserved
 * when the file is regenerated.
 */

#include "mqtt.h"

/* $header() */
#include "mosquitto.h"
/* $end */

int mqttMain(int argc, char* argv[]) {
/* $begin(main) */
    /* Insert code that must be run when component is loaded */
    CORTO_UNUSED(argc);
    CORTO_UNUSED(argv);
    mosquitto_lib_init();
    return 0;
/* $end */
}
