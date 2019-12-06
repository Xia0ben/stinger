import ctypes
import os
import functools
from stinger_core import Stinger


# You must set the STINGER_LIB_PATH environment variable to be able to use this wrapper. You can do so by executing:
# export STINGER_LIB_PATH=<REPLACE-BY-ABSOLUTE-PATH-TO-STINGER-FOLDER>/stinger/build/lib/
if os.getenv('STINGER_LIB_PATH'):
    libstinger_net = ctypes.cdll.LoadLibrary(os.getenv('STINGER_LIB_PATH') + '/libstinger_net.so')
else:
    libstinger_net = ctypes.cdll.LoadLibrary('libstinger_net.so')


class StingerAlgParams(ctypes.Structure):
    """
    Stinger algorithm parameters data structure
    See /lib/stinger_net/inc/stinger_alg.h for full documentation on the parameters meanings.
    """
    _fields_ = [("name", ctypes.c_char_p),
                ("host", ctypes.c_char_p),
                ("port", ctypes.c_int),
                ("is_remote", ctypes.c_int),
                ("map_private", ctypes.c_int),
                ("data_per_vertex", ctypes.c_int64),
                ("data_description", ctypes.c_char_p),
                ("dependencies", ctypes.c_void_p),
                ("num_dependencies", ctypes.c_int64)]


class StingerEdgeUpdate(ctypes.Structure):
    """
    Stinger edge update data structure
    """
    _fields_ = [("etype", ctypes.c_int64),
                ("etype_str", ctypes.c_char_p),
                ("source", ctypes.c_int64),
                ("source_str", ctypes.c_char_p),
                ("destination", ctypes.c_int64),
                ("destination_str", ctypes.c_char_p),
                ("weight", ctypes.c_int64),
                ("time", ctypes.c_int64),
                ("result", ctypes.c_int64),
                ("meta_index", ctypes.c_int64)]


class StingerVertexUpdate(ctypes.Structure):
    """
    Stinger vertex update data structure
    """
    _fields_ = [("vertex", ctypes.c_int64),
                ("vertex_str", ctypes.c_char_p),
                ("type", ctypes.c_int64),
                ("type_str", ctypes.c_char_p),
                ("set_weight", ctypes.c_int64),
                ("incr_weight", ctypes.c_int64),
                ("meta_index", ctypes.c_int64)]


class StingerRegisteredAlg(ctypes.Structure):
    """
    Stinger Algorithm data structure.
    Example usage available in src/clients/algorithms/simple_communities/src/simple_communities.c
    """
    _fields_ = [("enabled", ctypes.c_int),
                ("map_private", ctypes.c_int),
                ("sock", ctypes.c_int),
                ("stinger", ctypes.c_void_p),
                ("stinger_loc", 256 * ctypes.c_char),
                ("alg_name", 256 * ctypes.c_char),
                ("alg_num", ctypes.c_int64),
                ("alg_data_loc", 256 * ctypes.c_char),
                ("alg_data", ctypes.c_void_p),
                ("alg_data_per_vertex", ctypes.c_int64),
                ("dep_count", ctypes.c_int64),
                ("dep_name", ctypes.POINTER(ctypes.c_char_p)),
                ("dep_location", ctypes.POINTER(ctypes.c_char_p)),
                ("dep_data", ctypes.POINTER(ctypes.c_void_p)),
                ("dep_description", ctypes.POINTER(ctypes.c_char_p)),
                ("dep_data_per_vertex", ctypes.POINTER(ctypes.c_int64)),
                ("batch", ctypes.c_int64),
                ("num_insertions", ctypes.c_int64),
                ("insertions", ctypes.POINTER(StingerEdgeUpdate)),
                ("num_deletions", ctypes.c_int64),
                ("deletions", ctypes.POINTER(StingerEdgeUpdate)),
                ("num_vertex_updates", ctypes.c_int64),
                ("vertex_updates", ctypes.POINTER(StingerVertexUpdate)),
                ("num_metadata", ctypes.c_int64),
                ("metadata", ctypes.POINTER(ctypes.c_char_p)),
                ("metadata_lengths", ctypes.POINTER(ctypes.c_int64)),
                ("batch_storage", ctypes.c_void_p),
                ("batch_type", ctypes.c_int)]

    def get_alg_data(self, name, desc):
        """
        Get algorithm computed data as a StingerDataArray.
        :param name: field name in data computed by the algorithm
        :type name: str
        :param desc: data description string. See /lib/stinger_net/inc/stinger_alg.h for format meaning.
        :type desc: str
        :return: data array for the specified field name
        :rtype StingerDataArray
        """
        return StingerDataArray(self.alg_data, desc, name, Stinger(s=self.stinger))

    def stinger(self):
        """
        Get stinger structure associated with the algorithm.
        :return: stinger structure associated with the algorithm
        :rtype: Stinger
        """
        return Stinger(s=self.stinger)


class StingerStream:
    """
    Stinger Stream allows to prepare and send batches of graph updates to the stinger server
    """
    def __init__(self, host='localhost', port=10102, strings=True, undirected=False):
        """
        Initialize connection with stinger server from host and port. Set default batch size to 5000 for each action.
        :param host: stinger server host IP address
        :type host: str
        :param port: stinger server port on host
        :type port: int
        :param strings: specifies default behavior when reading vertex id : interpret it as an int id or a str name
        :type strings: bool
        :param undirected: specifies whether the updated graph is directed or not, set to directed as default.
        :type undirected: bool
        """
        self.sock_handle = libstinger_net['stream_connect'](ctypes.c_char_p(host), ctypes.c_int(port))
        self.insertions_size = 5000
        self.insertions = (StingerEdgeUpdate * self.insertions_size)()
        self.insertions_refs = []
        self.insertions_count = 0
        self.deletions_size = 5000
        self.deletions = (StingerEdgeUpdate * self.deletions_size)()
        self.deletions_refs = []
        self.deletions_count = 0
        self.vertex_updates_size = 5000
        self.vertex_updates = (StingerVertexUpdate * self.deletions_size)()
        self.vertex_updates_refs = []
        self.vertex_updates_count = 0
        self.only_strings = strings
        self.undirected = undirected

    def add_insert(self, vfrom, vto, etype=0, weight=0, ts=0, insert_strings=None):
        """
        Add an edge insertion action to the current batch.
        :param vfrom: "from" vertex index if int, vertex unique name identifier if str
        :type vfrom: int or str
        :param vto: "to" vertex index if int, vertex unique name identifier if str
        :type vto: int or str
        :param etype: edge type (must be previously declared by calling stinger_core.create_etype)
        :type etype: int
        :param weight: edge weight
        :type weight: int
        :param ts: timestamp
        :type ts: int
        :param insert_strings: specifies behavior when reading vertex id : interpret it as an int id or a str name
        :type insert_strings: bool
        """
        self.only_strings = insert_strings if insert_strings is not None else self.only_strings

        if self.insertions_count >= self.insertions_size:
            self.insertions_size *= 2
            insertions_tmp = (StingerEdgeUpdate * self.insertions_size)()
            self.insertions_refs.append(self.insertions)
            ctypes.memmove(ctypes.addressof(insertions_tmp), ctypes.addressof(self.insertions),
                           ctypes.sizeof(StingerEdgeUpdate * (self.insertions_size / 2)))
            self.insertions = insertions_tmp

        if self.only_strings:
            self.insertions[self.insertions_count].source_str = ctypes.c_char_p(vfrom)
            self.insertions[self.insertions_count].destination_str = ctypes.c_char_p(vto)
        else:
            self.insertions[self.insertions_count].source_str = 0
            self.insertions[self.insertions_count].destination_str = 0
            self.insertions[self.insertions_count].source = ctypes.c_int64(vfrom)
            self.insertions[self.insertions_count].destination = ctypes.c_int64(vto)

        if isinstance(etype, str):
            self.insertions[self.insertions_count].etype_str = ctypes.c_char_p(etype)
        else:
            self.insertions[self.insertions_count].etype = ctypes.c_int64(etype)

        self.insertions[self.insertions_count].weight = ctypes.c_int64(weight)
        self.insertions[self.insertions_count].time = ctypes.c_int64(ts)

        self.insertions_count += 1

    def add_delete(self, vfrom, vto, etype=0):
        """
        Add an edge deletion action to the current batch.
        :param vfrom: "from" vertex index if int, vertex unique name identifier if str
        :type vfrom: int or str
        :param vto: "to" vertex index if int, vertex unique name identifier if str
        :type vto: int or str
        :param etype: edge type (must be previously declared by calling stinger_core.create_etype)
        :type etype: int
        """
        if self.deletions_count >= self.deletions_size:
            self.deletions_size *= 2
            deletions_tmp = (StingerEdgeUpdate * self.deletions_size)()
            self.deletions_refs.append(self.deletions)
            ctypes.memmove(ctypes.addressof(deletions_tmp), ctypes.addressof(self.deletions),
                           ctypes.sizeof(StingerEdgeUpdate * (self.deletions_size / 2)))
            self.deletions = deletions_tmp

        if self.only_strings:
            self.deletions[self.deletions_count].source_str = ctypes.c_char_p(vfrom)
            self.deletions[self.deletions_count].destination_str = ctypes.c_char_p(vto)
        else:
            self.deletions[self.deletions_count].source_str = 0
            self.deletions[self.deletions_count].destination_str = 0
            self.deletions[self.deletions_count].source = ctypes.c_int64(vfrom)
            self.deletions[self.deletions_count].destination = ctypes.c_int64(vto)

        if isinstance(etype, str):
            self.deletions[self.deletions_count].etype_str = ctypes.c_char_p(etype)
        else:
            self.deletions[self.deletions_count].etype = ctypes.c_int64(etype)

        self.deletions_count += 1

    def add_vertex_update(self, vtx, vtype, weight=0, incr_weight=0):
        """
        Add a vertex update action to the current batch.
        :param vtx: vertex index if int, vertex unique name identifier if str
        :type vtx: int or str
        :param vtype: vertex type (must be previously declared by calling stinger_core.create_vtype)
        :type vtype: int
        :param weight: vertex weight
        :type weight: int
        :param incr_weight: increment to current vertex weight
        :type incr_weight: int
        """
        if self.vertex_updates_count >= self.vertex_updates_size:
            self.vertex_updates_size *= 2
            vertex_updates_tmp = (StingerVertexUpdate * self.vertex_updates_size)()
            self.vertex_updates_refs.append(self.vertex_updates)
            ctypes.memmove(ctypes.addressof(vertex_updates_tmp), ctypes.addressof(self.vertex_updates),
                           ctypes.sizeof(StingerVertexUpdate * (self.vertex_updates_size / 2)))
            self.vertex_updates = vertex_updates_tmp

        if self.only_strings:
            self.vertex_updates[self.vertex_updates_count].vertex_str = ctypes.c_char_p(vtx)
        else:
            self.vertex_updates[self.vertex_updates_count].vertex = ctypes.c_int64(vtx)
            self.vertex_updates[self.vertex_updates_count].vertex_str = 0

        if isinstance(vtype, str):
            self.vertex_updates[self.vertex_updates_count].type_str = ctypes.c_char_p(vtype)
        else:
            self.vertex_updates[self.vertex_updates_count].type = ctypes.c_int64(vtype)

        self.vertex_updates[self.vertex_updates_count].weight = ctypes.c_int64(weight)
        self.vertex_updates[self.vertex_updates_count].incr_weight = ctypes.c_int64(incr_weight)

        self.vertex_updates_count += 1

    def send_batch(self):
        """
        Send registered edge insertions, deletions and vertex updates to the Stinger server.
        Blocking call until all insertions, deletions and vertex updates have been processed.
        Resets counts and refs lists.
        """
        libstinger_net['stream_send_batch'](
            ctypes.c_int(self.sock_handle), ctypes.c_int(self.only_strings),
            self.insertions, ctypes.c_int64(self.insertions_count),
            self.deletions, ctypes.c_int64(self.deletions_count),
            self.vertex_updates, ctypes.c_int64(self.vertex_updates_count),
            ctypes.c_bool(self.undirected))
        self.insertions_count = 0
        self.deletions_count = 0
        self.vertex_updates_count = 0
        self.insertions_refs = []
        self.deletions_refs = []
        self.vertex_updates_refs = []


class StingerAlg:
    """
    Stinger Algorithm interface
    To be used when implementing an algorithm that uses a stinger data structure.
    """
    def __init__(self, params):
        register = libstinger_net['stinger_register_alg_impl']
        register.argtypes = [StingerAlgParams]
        register.restype = ctypes.POINTER(StingerRegisteredAlg)
        self.alg_ptr = register(params)
        self.alg = self.alg_ptr[0]

    def begin_init(self):
        """
        Request to begin the static initialization phase.
        This is a blocking call that sends a request to the server to obtain the next batch of updates. Between the
        time that this returns and the time that *end_init is called, the STINGER structure is guaranteed to be static.
        See /lib/stinger_net/inc/stinger_alg.h for partial documentation.
        """
        libstinger_net['stinger_alg_begin_init'](self.alg_ptr)

    def end_init(self):
        """
        Request to end the static initialization phase.
        Static initialization code must always be between calls to begin_init and end_init.
        See /src/clients/algorithms/simple_communities/src/simple_communities.c for a good usage example.
        """
        libstinger_net['stinger_alg_end_init'](self.alg_ptr)

    def begin_pre(self):
        """
        Request to begin the pre processing phase.
        See /src/clients/algorithms/simple_communities/src/simple_communities.c for a good usage example.
        """
        libstinger_net['stinger_alg_begin_pre'](self.alg_ptr)

    def end_pre(self):
        """
        Request to end the pre processing phase.
        Pre processing code must always be between calls to begin_post and end_post.
        See /src/clients/algorithms/simple_communities/src/simple_communities.c for a good usage example.
        """
        libstinger_net['stinger_alg_end_pre'](self.alg_ptr)

    def begin_post(self):
        """
        Request to begin the post processing phase.
        See /src/clients/algorithms/simple_communities/src/simple_communities.c for a good usage example.
        """
        libstinger_net['stinger_alg_begin_post'](self.alg_ptr)

    def end_post(self):
        """
        Request to end the post processing phase.
        Post processing code must always be between calls to begin_post and end_post.
        See /src/clients/algorithms/simple_communities/src/simple_communities.c for a good usage example.
        """
        libstinger_net['stinger_alg_end_post'](self.alg_ptr)

    def stinger(self):
        """
        Get stinger structure associated with the algorithm.
        :return: stinger structure associated with the algorithm
        :rtype: Stinger
        """
        return Stinger(s=self.alg.stinger)


class StingerDataArray:
    """
    Commodity class to access vertices data.
    """
    def __init__(self, data_ptr, data_desc, field_name, s):
        """
        Initialize algorithm computed data array for the given field_name, by computing the data_type from
        the data_desc and the data_array pointer from the max number of vertices.
        :param data_ptr: Pointer to the algorithm's computed data
        :type data_ptr: ctypes.c_void_p (void pointer)
        :param data_desc: data description string. See /lib/stinger_net/inc/stinger_alg.h for format meaning.
        :type data_desc: str
        :param field_name: field name in the data description that will be used to extract the data array
        :type field_name: str
        :param s: stinger data structure
        :type s: Stinger
        """
        data_desc = data_desc.split()

        if not isinstance(data_ptr, int):
            data_ptr = data_ptr.value

        print(data_desc)
        field_index = data_desc[1:].index(field_name)

        self.field_name = field_name
        self.data_type = data_desc[0][field_index]
        self.nv = libstinger_net['stinger_alg_max_vertices'](s.raw())
        self.s = s

        offset = functools.reduce(
            lambda x, y: x + y,
            [8 if c == 'd' or c == 'l' else
             4 if c == 'f' or c == 'i' else
             1
             for c in data_desc[0][:field_index]],
            0)

        self.data = data_ptr + (offset * self.nv)

        if self.data_type == 'd':
            self.data = ctypes.cast(self.data, ctypes.POINTER(ctypes.c_double))
        elif self.data_type == 'f':
            self.data = ctypes.cast(self.data, ctypes.POINTER(ctypes.c_float))
        elif self.data_type == 'l':
            self.data = ctypes.cast(self.data, ctypes.POINTER(ctypes.c_int64))
        elif self.data_type == 'i':
            self.data = ctypes.cast(self.data, ctypes.POINTER(ctypes.c_int32))
        else:  # self.data_type == 'b'
            self.data = ctypes.cast(self.data, ctypes.POINTER(ctypes.c_int8))

    def __getitem__(self, i):
        """
        Allows to get array items by using the [] operator
        :param i: vertex index if int, vertex unique name identifier if str
        :type i: int or str
        :return: data field value at i if found,
        0 otherwise (only happens if i < 0 or if i is instance of str and mapping to the corresponding vertex id fails.
        :rtype: c_double/c_float/c_int64/c_int32/c_int8 depending on associated field type
        """
        if isinstance(i, str):
            i = self.s.get_mapping(i)
        return self.data[i] if i >= 0 else 0

    def __setitem__(self, i, k):
        """
        Affect value k at index i using []= operator
        :param i: vertex index if int, vertex unique name identifier if str
        :type i: int or str
        :param k: field data to insert at i
        :type k: c_double/c_float/c_int64/c_int32/c_int8 depending on associated field type
        :return: k if setting is successful,
        0 otherwise (only happens if i < 0 or if i is instance of str and mapping to the corresponding vertex id fails.
        :rtype: c_double/c_float/c_int64/c_int32/c_int8 depending on associated field type
        """
        if isinstance(i, str):
            i = self.s.get_mapping(i)
        if i >= 0:
            self.data[i] = k
            return k
        else:
            return 0

    def get_data_ptr(self):
        """
        Get pointer to array data (offseted from original algorithm computed data pointer)
        :return: ctypes.POINTER(ctypes.c_double/c_float/c_int64/c_int32/c_int8) depending on associated field type
        """
        return self.data

    def weight_double(self, weight):
        """
        Apply (multiply by) a weight on the computed data field
        :param weight: multiplication coefficient
        """
        libstinger_net['stinger_alg_weight_double'](self.s.raw(), self.data, ctypes.c_double(weight))


class StingerAlgState:
    """
    Stinger algorithm state data structure.
    """
    def __init__(self, alg, stinger):
        """
        Setup algorithm state.
        :param alg: algorithm structure
        :type alg: StingerAlg
        :param stinger: stinger data structure
        :type stinger: Stinger
        """
        self.alg = alg
        self.s = stinger

    def get_name(self):
        """
        Get the unique name identifying the algorithm to the server and other running algorithms. Note that this
        name must be less than 255 characters and must contain only the characters a-z, A-Z, 0-9, and _.
        :return: algorithm unique name identifier
        :rtype: str
        """
        name = libstinger_net['stinger_alg_state_get_name']
        name.restype = ctypes.c_char_p
        return str(name(self.alg))

    def get_data_description(self):
        """
        Get algorithm data description, that is, a description of the kind of data stored per each vertex.
        This is used to enable automated parsing and printing of the data.
        See /lib/stinger_net/inc/stinger_alg.h or StingerDataArray.__init__ method above for the detailed meaning.
        :return: algorithm data description
        :rtype: str
        """
        dd = libstinger_net['stinger_alg_state_get_data_description']
        dd.restype = ctypes.c_char_p
        return str(dd(self.alg))

    def get_data_ptr(self):
        """
        Get pointer to the data computed by the algorithm.
        :return: pointer to computed data
        :rtype: ctypes.c_void_p (void pointer)
        """
        dp = libstinger_net['stinger_alg_state_get_data_ptr']
        dp.restype = ctypes.c_void_p
        return ctypes.c_void_p(dp(self.alg))

    def get_data_array(self, name):
        """
        Get algorithm computed data as a StingerDataArray.
        :param name: field name in data computed by the algorithm
        :type name: str
        :return: data array for the specified field name
        :rtype StingerDataArray
        """
        return StingerDataArray(self.get_data_ptr(), self.get_data_description(), name, self.s)

    def get_data_per_vertex(self):
        """
        Get the amount of data stored by the algorithm for each vertex in bytes.
        See /lib/stinger_net/inc/stinger_alg.h for more details.
        :return: amount of data stored by the algorithm for each vertex in bytes.
        :rtype: int
        """
        return libstinger_net['stinger_alg_state_data_per_vertex'](self.alg)

    def get_level(self):
        """
        Some incremental graph algorithms like Louvain Community Detection execute in levels. This function allows to
        know at which level the algorithm is at during its execution.
        Cf. src/clients/algorithms/community_detection/src/community_detection.c
        Cf. src/server/src/alg_handling.cpp
        :return: algorithm execution level
        :rtype: int
        """
        return libstinger_net['stinger_alg_state_level'](self.alg)

    def number_of_dependencies(self):
        """
        Get the count of the number of dependencies the algorithm's parameters dependencies field.
        :return: number of algorithm dependencies
        """
        return libstinger_net['stinger_alg_state_number_dependencies'](self.alg)

    def get_dependency(self, i):
        """
        Get algorithm dependency at index i.
        :param i: index to dependency
        :return: dependency name
        :rtype: str
        """
        dep = libstinger_net['stinger_alg_state_depencency']
        dep.restype = ctypes.c_char_p
        return dep(self.alg, ctypes.c_int64(i))


class StingerMon:
    """
    Stinger structure Monitor (Mon).
    Allows to check how many, which and the state of any running algorithms on the STINGER structure.
    Also allows to obtain/release a lock on said structure.
    """
    def __init__(self, name, host='localhost', port=10103):
        """
        Connect to running Stinger server as a Monitor.
        :param name: Monitor name (can be anything)
        :type name: str
        :param host: host IP address on which the server is running
        :type host: str
        :param port: host port
        :type port: int
        """
        libstinger_net['mon_connect'](ctypes.c_int(port), ctypes.c_char_p(host), ctypes.c_char_p(name))
        get_mon = libstinger_net['get_stinger_mon']
        get_mon.restype = ctypes.c_void_p
        self.mon = ctypes.c_void_p(get_mon())

    def num_algs(self):
        """
        Get number of algorithms currently running on the stinger structure.
        :return: number of algorithms currently running on the stinger structure.
        :rtype: int
        """
        return libstinger_net['stinger_mon_num_algs'](self.mon)

    def get_alg_state(self, name_or_int):
        """
        Get the current state of the algorithm.
        :param name_or_int: Algorithm name or int identifier
        :type name_or_int: str or int
        :return: Current state of the algorithm
        :rtype: StingerAlgState
        """
        if isinstance(name_or_int, str):
            get_alg = libstinger_net['stinger_mon_get_alg_state_by_name']
            get_alg.restype = ctypes.c_void_p
            return StingerAlgState(ctypes.c_void_p(get_alg(self.mon, ctypes.c_char_p(name_or_int))), self.stinger())
        else:
            get_alg = libstinger_net['stinger_mon_get_alg_state']
            get_alg.restype = ctypes.c_void_p
            return StingerAlgState(ctypes.c_void_p(get_alg(self.mon, ctypes.c_int64(name_or_int))), self.stinger())

    def has_alg(self, name):
        """
        Check if algorithm is running on the structure associated with the monitor.
        :param name: Algorithm name
        :type name: str
        :return: True if algorithm is running on the structure, False otherwise
        """
        return libstinger_net['stinger_mon_has_alg'](self.mon, ctypes.c_char_p(name))

    def get_read_lock(self):
        """
        Lock the stinger structure for reading.
        """
        libstinger_net['stinger_mon_get_read_lock'](self.mon)

    def release_read_lock(self):
        """
        Unlock the stinger structure after reading.
        """
        libstinger_net['stinger_mon_release_read_lock'](self.mon)

    def stinger(self):
        """
        Get stinger structure associated with the monitor.
        :return: stinger structure associated with the monitor
        :rtype: Stinger
        """
        get_stinger = libstinger_net['stinger_mon_get_stinger']
        get_stinger.restype = ctypes.c_void_p
        return Stinger(s=ctypes.c_void_p(get_stinger(self.mon)))

    def wait_for_sync(self):
        """
        Wait for the structure to be in a lockable state. To be used before getting the read lock.
        """
        libstinger_net['stinger_mon_wait_for_sync'](self.mon)

