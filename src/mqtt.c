/* mqtt.c
 *
 * This file contains the implementation for the generated interface.
 *
 * Don't mess with the begin and end tags, since these will ensure that modified
 * code in interface functions isn't replaced when code is re-generated.
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
