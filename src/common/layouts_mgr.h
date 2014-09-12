/*****************************************************************************\
 *  layouts_mgr.h - layouts manager data structures and main functions
 *****************************************************************************
 *  Initially written by Francois Chevallier <chevallierfrancois@free.fr>
 *  at Bull for slurm-2.6.
 *  Adapted by Matthieu Hautreux <matthieu.hautreux@cea.fr> for slurm-14.11.
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#ifndef __LAYOUTS_MGR_1NRINRSD__INC__
#define __LAYOUTS_MGR_1NRINRSD__INC__

#include "src/common/list.h"
#include "src/common/xhash.h"
#include "src/common/xtree.h"
#include "src/common/parse_config.h"

#include "src/common/layout.h"
#include "src/common/entity.h"

/*
 * Layouts are managed through a "layouts manager" of type layouts_mgr_t.
 *
 * The layouts_mgr_t manages the layouts and entities loaded through the list
 * of layouts specified in the Slurm configuration file (slurm.conf)
 *
 * At startup, Slurm initialize one layouts_mgr_t using slurm_layouts_init()
 * and then load the required layouts defined in the configuration using
 * slurm_layouts_load_config().
 *
 * The different layouts and entities can then be queried using either
 * layouts_get_layout() and layouts_get_entity().
 *
 * Note that each entity contains a list of nodes appearing inside the
 * associated layouts.
 */

/*
 * Potential enhancement to complete: agregate specified plugin etypes in a
 *      xhash in the mgr, avoiding same string to be duplicated again and again.
 *      (in short: apply the same logic for etypes as for entity data keys.)
 */

/*Get or Set function*/
#define LAYOUTS_API_SET 1
#define LAYOUTS_API_GET 2

/*Direction for set/consolidation*/
#define LAYOUTS_SET_DIRECTION_NONE       0x00000001
#define LAYOUTS_SET_DIRECTION_SAVE       0x00000002
#define LAYOUTS_SET_DIRECTION_UP         0x00000004
#define LAYOUTS_SET_DIRECTION_DOWN       0x00000008

/*Operation for set function*/
#define LAYOUTS_SET_OPERATION_SET        0x00000010
#define LAYOUTS_SET_OPERATION_SUM        0x00000020

/*Way to consolidate*/
#define LAYOUTS_SET_CONSOLIDATION_SUM    0x00000100
#define LAYOUTS_SET_CONSOLIDATION_MEAN   0x00000200
#define LAYOUTS_SET_CONSOLIDATION_SET    0x00000400

typedef enum layouts_keydef_types_en {
	L_T_ERROR = 0,
	L_T_STRING,
	L_T_LONG,
	L_T_UINT16,
	L_T_UINT32,
	L_T_BOOLEAN,
	L_T_FLOAT,
	L_T_DOUBLE,
	L_T_LONG_DOUBLE,
	L_T_CUSTOM,
} layouts_keydef_types_t;

typedef struct layouts_keyspec_st {
	char*			key;
	layouts_keydef_types_t	type;
	void			(*custom_destroy)(void*);
	char*			(*custom_dump)(void*);
} layouts_keyspec_t;

typedef struct layouts_plugin_spec_st {
	const s_p_options_t*		options;
	const layouts_keyspec_t*	keyspec;
	int				struct_type;
	const char**			etypes;
	bool				automerge;
} layouts_plugin_spec_t;

/*****************************************************************************\
 *                             PLUGIN FUNCTIONS                              *
\*****************************************************************************/

/*
 * slurm_layouts_init - intialize the layouts mgr, load the required plugins
 *        and initialize the internal hash tables for entities, keydefs and
 *        layouts.
 *
 * Return SLURM_SUCCESS or SLURM_ERROR if all the required layouts were not
 * loaded correctly.
 *
 * Notes: this call do not try to read and parse the layouts configuration
 * files. It only loads the layouts plugins, dlsym the layout API and conf
 * elements to prepare the reading and parsing performed in the adhoc call
 * slurm_layouts_load_config()
 *
 */
int slurm_layouts_init(void);

/*
 * slurm_layouts_fini - uninitialize the layouts mgr and free the internal
 *        hash tables.
 */
int slurm_layouts_fini(void);

/*
 * slurm_layouts_load_config - use the layouts plugins details loaded during
 *        slurm_layouts_init() and read+parse the different layouts
 *        configuration files, creating the entities and the relational
 *        structures associated the eaf of them.
 *
 * Return SLURM_SUCCESS or SLURM_ERROR if all the required layouts were not
 * loaded correctly.
 */
int slurm_layouts_load_config(void);

/*
 * layouts_get_layout - return the layout from a given type
 *
 * Return a pointer to the layout_t struct of the layout or NULL if not found
 */
layout_t* layouts_get_layout(const char* type);

/*
 * layouts_get_entity - return the entity from a given name
 *
 * Return a pointer to the entity_t struct of the entity or NULL if not found
 */
entity_t* layouts_get_entity(const char* name);


/*API*/
/*  layouts_api(layouts_api_set_get,layout_type,key_type,
                entities_names,entities_struct,consolidation_layout,vector)
   Set/Get/Update a layout data for listed entities.
   IN     layouts_api_set_get   - define if call must set/updpate or get/update
   IN     layout_type           - type of layout
   IN     key_type              - name of the data to get/set
   IN     entities_names        - name of all entities to set/get (optional)
   				  (ended by NULL)
   IN/OUT entities_struct       - IN: use instead of entities_names to save time
                                  OUT: if direction is SAVE, pointers to
                                       entities declared in entities_names
   				  (ended by NULL)
   IN     consolidation_layout  - operation/direction/consolidation options
   IN/OUT vector                  IN: for set/update, vector to set/add to data
                                  OUT:  for get/update, data of each entities
                                  NULL: for (set)update, consolidate from listed
                                        entities
 */
int layouts_api( int layouts_api_set_get,
		 const char *layout_type,
		 const char* key_type,
		 const char** entities_names,
		 entity_t** entities_struct,
		 int consolidation_layout,
		 void* vector);

/* API wrapper (does doesn't allow SAVE entities_struct
 * 5 functions to set, get, update, update/get, propagate(set+update)
*/
/*  layouts_api_get_value(layout_type,entities_names,key_type,vector)
   Get layout data for listed entities.
   IN   layout_type     - type of layout
   IN   entities_names  - name of entities (ended by NULL)
   IN   key_type        - name of the data to update
   OUT  vector          - data to get
 */
int layouts_api_get_value(  const char *layout_type,
			    const char** entities_names,
			    const char* key_type,
			    void* vector);
/*  layouts_api_set_value(layout_type,entities_names,key_type,operation,vector)
   Set/update layout data for listed entities.
   IN   layout_type     - type of layout
   IN   entities_names  - name of entities (ended by NULL)
   IN   key_type        - name of the data to update
   IN   operation       - LAYOUTS_SET_OPERATION_*
   IN   vector          - data to add/set to entities
 */
int layouts_api_set_value(  const char *layout_type,
			    const char** entities_names,
			    const char* key_type,
			    int operation,
			    void* vector);
/*  layouts_api_update_value_from(layout_type,entities_names,key_type,direction,
				  consolidation)
   Update a layout data from listed entities.
   IN   layout_type     - type of layout
   IN   entities_names  - name of entities root for the update (ended by NULL)
   IN   key_type        - name of the data to update
   IN   direction       - LAYOUTS_SET_DIRECTION_* (DOWN or UP)
   IN   consolidation   - LAYOUTS_SET_CONSOLIDATION_*
 */
int layouts_api_update_value_from(  const char *layout_type,
				    const char** entities_names,
				    const char* key_type,
				    int direction,
				    int consolidation);
/*  layouts_api_get_updated_value(layout_type,entities_names,key_type,direction,
				  consolidation,vector)
   Get updated layout data for listed entities.
   IN   layout_type     - type of layout
   IN   entities_names  - name of entities root for the update (ended by NULL)
   IN   key_type        - name of the data to update
   IN   direction       - LAYOUTS_SET_DIRECTION_* (DOWN or UP)
   IN   consolidation   - LAYOUTS_SET_CONSOLIDATION_*
   OUT  vector          - data to get
 */
int layouts_api_get_updated_value( const char *layout_type,
				   const char** entities_names,
				   const char* key_type,
				   int direction,
				   int consolidation,
				   void* vector);
/*  layouts_api_propagate_value(layout_type,entities_names,key_type,operation,
				direction,consolidation,vector)
   Set/update layout data for listed entities.and consolidate (update) other
   entities
   IN   layout_type     - type of layout
   IN   entities_names  - name of entities root for the update (ended by NULL)
   IN   key_type        - name of the data to update
   IN   operation       - LAYOUTS_SET_OPERATION_*
   IN   direction       - LAYOUTS_SET_DIRECTION_* (DOWN or UP)
   IN   consolidation   - LAYOUTS_SET_CONSOLIDATION_*
   IN   vector          - data to add/set to entities
 */
int layouts_api_propagate_value( const char *layout_type,
				 const char** entities_names,
				 const char* key_type,
				 int operation,
				 int direction,
				 int consolidation,
				 void* vector);

/*  layouts_api_list_entities(layout_type,entity_type,value_type,entities_name)
   get name of entities regarding type, value_type and layout
   Set/update layout data for listed entities.and consolidate (update) other
   entities
   IN   layout_type     - type of layout
   IN   entity_type     - type of entities (optional if value_type)
   IN   value_type      - type of value (optional if entity_type)
   OUT  entities_name   - names of entities
   RETURN               - number of return entities
 */
int layouts_api_list_entities(  const char *layout_type,
				const char *entity_type,
				const char *value_type,
				char **entities_name);

/*  layouts_api_get_values(layout_type,entity_name,entities_struct,
			   key_type,vector)
   get several values from keys of same type of one layout for one entity
   IN   layout_type	- type of layout
   IN	entity_name	- name of the entity (NULL if entities_struct)
   IN	entities_struct	- entity (NULL if use name)
   IN	key_types	- all keys
   OUT	vector		- the return values
   RETURN success or error
*/
int layouts_api_get_values( char *layout_type,
			    const char* entity_name,
			    entity_t* entities_struct,
			    const char** key_type,
			    void* vector);

#endif /* end of include guard: __LAYOUTS_MGR_1NRINRSD__INC__ */
