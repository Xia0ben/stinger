import ctypes
import os


# You must set the STINGER_LIB_PATH environment variable to be able to use this wrapper. You can do so by executing:
# export STINGER_LIB_PATH=<REPLACE-BY-ABSOLUTE-PATH-TO-STINGER-FOLDER>/stinger/build/lib/
if os.getenv('STINGER_LIB_PATH'):
    libstinger_core = ctypes.cdll.LoadLibrary(os.getenv('STINGER_LIB_PATH') + '/libstinger_core.so')
else:
    libstinger_core = ctypes.cdll.LoadLibrary('libstinger_core.so')


class Stinger:
    """
    STINGER (Spatio-Temporal Interaction Networks and Graphs Extensible Representation) interface
    """
    def __init__(self, s=None, filename=None):
        """
        Initialize stinger structure:
        If a filename is provided, it will always be used to try and create the stinger structure from the file data.
        If an existing structure is provided, the created object will simply refer to it.
        :param s: existing stinger structure
        :type s: Stinger
        :param filename: absolute path to stinger saved file
        :type filename: str
        """
        if filename is not None:
            self.s = ctypes.c_void_p(0)
            nv = ctypes.c_int64(0)
            libstinger_core['stinger_open_from_file'](
                ctypes.c_char_p(filename), ctypes.c_void_p(ctypes.addressof(self.s)),
                ctypes.c_void_p(ctypes.addressof(nv)))
            self.free = True
        elif s is None:
            stinger_new = libstinger_core['stinger_new']
            stinger_new.restype = ctypes.c_void_p
            self.free = True
            self.s = ctypes.c_void_p(stinger_new())
        else:
            self.free = False
            self.s = ctypes.c_void_p(s)

    def __del__(self):
        """
        Delete the stinger data structure.
        """
        if self.free:
            stinger_free_all = libstinger_core['stinger_free_all']
            stinger_free_all.restype = ctypes.c_void_p
            self.s = stinger_free_all(self.s)

    def raw(self):
        """
        Return the pointer to the stinger data structure
        :return: pointer to the stinger data structure
        :rtype: ctypes.c_void_p
        """
        return self.s

    def save_to_file(self, filename):
        """
        Checkpoint a stinger data structure to disk.
        :param filename: the path and name of the output file
        :type filename: str
        """
        libstinger_core['stinger_save_to_file'](self.s, 1 + self.max_active_vtx(), ctypes.c_char_p(filename))

    def max_active_vtx(self):
        """
        Finds the largest vertex ID whose in-degree and/or out-degree is greater than zero.
        :return: largest active vertex ID
        :rtype: int
        """
        return libstinger_core['stinger_max_active_vertex'](self.s)

    def create_mapping(self, name):
        """

        :param name:
        :type name:
        :return: Vertex id
        :rtype: int
        """
        vtx_out = ctypes.c_int64(0)
        libstinger_core['stinger_mapping_create'](
            self.s, ctypes.c_char_p(name), len(name), ctypes.c_void_p(ctypes.addressof(vtx_out)))
        return vtx_out.value

    def get_mapping(self, name):
        """
        Get vertex int id per unique name identifier
        :param name: unique name identifier for vertex
        :return: int
        """
        return libstinger_core['stinger_mapping_lookup'](self.s, ctypes.c_char_p(name), len(name))

    def get_name(self, vtx):
        name = ctypes.c_char_p(0)
        length = ctypes.c_int64(0)
        libstinger_core['stinger_mapping_physid_direct'](
            self.s, ctypes.c_int64(vtx), ctypes.c_void_p(ctypes.addressof(name)),
            ctypes.c_void_p(ctypes.addressof(length)))
        rtn = str(name.value[:length.value])
        return rtn

    def mapping_nv(self):
        return libstinger_core['stinger_mapping_nv'](self.s)

    def create_vtype(self, name):
        vtx_out = ctypes.c_int64(0)
        libstinger_core['stinger_vtype_names_create_type'](
            self.s, ctypes.c_char_p(name), ctypes.c_void_p(ctypes.addressof(vtx_out)))
        return vtx_out.value

    def get_vtype(self, name):
        return libstinger_core['stinger_vtype_names_lookup_type'](self.s, ctypes.c_char_p(name))

    def get_vtype_name(self, vtype):
        lookup_name = libstinger_core['stinger_vtype_names_lookup_name']
        lookup_name.restype = ctypes.c_char_p
        return lookup_name(self.s, ctypes.c_int64(vtype))

    def num_vtypes(self):
        return libstinger_core['stinger_vtype_names_count'](self.s)

    def create_etype(self, name):
        vtx_out = ctypes.c_int64(0)
        libstinger_core['stinger_etype_names_create_type'](
            self.s, ctypes.c_char_p(name), ctypes.c_void_p(ctypes.addressof(vtx_out)))
        return vtx_out.value

    def get_etype(self, name):
        return libstinger_core['stinger_etype_names_lookup_type'](self.s, ctypes.c_char_p(name))

    def get_etype_name(self, etype):
        lookup_name = libstinger_core['stinger_etype_names_lookup_name']
        lookup_name.restype = ctypes.c_char_p
        return lookup_name(self.s, ctypes.c_int64(etype))

    def num_etypes(self):
        return libstinger_core['stinger_etype_names_count'](self.s)

    def insert_edge(self, vfrom, vto, etype=0, weight=1, ts=1):
        if isinstance(vfrom, str):
            vfrom = self.create_mapping(vfrom)
        if isinstance(vto, str):
            vto = self.create_mapping(vto)
        if isinstance(etype, str):
            etype = self.create_etype(etype)
        libstinger_core['stinger_insert_edge'](self.s, ctypes.c_int64(etype), ctypes.c_int64(vfrom),
                                               ctypes.c_int64(vto), ctypes.c_int64(weight), ctypes.c_int64(ts))

    def insert_edge_pair(self, vfrom, vto, etype=0, weight=1, ts=1):
        if isinstance(vfrom, str):
            vfrom = self.create_mapping(vfrom)
        if isinstance(vto, str):
            vto = self.create_mapping(vto)
        if isinstance(etype, str):
            etype = self.create_etype(etype)
        libstinger_core['stinger_insert_edge_pair'](self.s, ctypes.c_int64(etype), ctypes.c_int64(vfrom),
                                                    ctypes.c_int64(vto), ctypes.c_int64(weight), ctypes.c_int64(ts))

    def increment_edge(self, vfrom, vto, etype=0, weight=1, ts=1):
        if isinstance(vfrom, str):
            vfrom = self.create_mapping(vfrom)
        if isinstance(vto, str):
            vto = self.create_mapping(vto)
        if isinstance(etype, str):
            etype = self.create_etype(etype)
        libstinger_core['stinger_incr_edge'](self.s, ctypes.c_int64(etype), ctypes.c_int64(vfrom),
                                             ctypes.c_int64(vto), ctypes.c_int64(weight), ctypes.c_int64(ts))

    def increment_edge_pair(self, vfrom, vto, etype=0, weight=1, ts=1):
        if isinstance(vfrom, str):
            vfrom = self.create_mapping(vfrom)
        if isinstance(vto, str):
            vto = self.create_mapping(vto)
        if isinstance(etype, str):
            etype = self.create_etype(etype)
        libstinger_core['stinger_incr_edge_pair'](self.s, ctypes.c_int64(etype), ctypes.c_int64(vfrom),
                                                  ctypes.c_int64(vto), ctypes.c_int64(weight), ctypes.c_int64(ts))

    def remove_edge(self, vfrom, vto, etype=0):
        if isinstance(vfrom, str):
            vfrom = self.get_mapping(vfrom)
        if isinstance(vto, str):
            vto = self.get_mapping(vto)
        if isinstance(etype, str):
            etype = self.create_etype(etype)
        if (vfrom > 0) and (vto > 0):
            libstinger_core['stinger_remove_edge'](self.s, ctypes.c_int64(etype), ctypes.c_int64(vfrom),
                                                   ctypes.c_int64(vto))

    def remove_edge_pair(self, vfrom, vto, etype=0):
        if isinstance(vfrom, str):
            vfrom = self.get_mapping(vfrom)
        if isinstance(vto, str):
            vto = self.get_mapping(vto)
        if isinstance(etype, str):
            etype = self.create_etype(etype)
        if (vfrom > 0) and (vto > 0):
            libstinger_core['stinger_remove_edge_pair'](self.s, ctypes.c_int64(etype), ctypes.c_int64(vfrom),
                                                        ctypes.c_int64(vto))

    def indegree(self, vtx):
        if isinstance(vtx, str):
            vtx = self.get_mapping(vtx)
        return libstinger_core['stinger_indegree_get'](self.s, ctypes.c_int64(vtx))

    def outdegree(self, vtx):
        if isinstance(vtx, str):
            vtx = self.get_mapping(vtx)
        return libstinger_core['stinger_outdegree_get'](self.s, ctypes.c_int64(vtx))

    def get_type(self, vtx):
        if isinstance(vtx, str):
            vtx = self.get_mapping(vtx)
        return libstinger_core['stinger_vtype_get'](self.s, ctypes.c_int64(vtx))

    def set_vtype(self, vtx, vtype):
        if isinstance(vtx, str):
            vtx = self.get_mapping(vtx)
        if isinstance(vtype, str):
            vtype = self.create_vtype(vtype)
        return libstinger_core['stinger_vtype_set'](self.s, ctypes.c_int64(vtx), ctypes.c_int64(vtype))

    def get_vweight(self, vtx):
        if isinstance(vtx, str):
            vtx = self.get_mapping(vtx)
        return libstinger_core['stinger_vweight_get'](self.s, ctypes.c_int64(vtx))

    def set_vweight(self, vtx, vweight):
        if isinstance(vtx, str):
            vtx = self.get_mapping(vtx)
        return libstinger_core['stinger_vweight_set'](self.s, ctypes.c_int64(vtx), ctypes.c_int64(vweight))

    def increment_vweight(self, vtx, vweight):
        if isinstance(vtx, str):
            vtx = self.get_mapping(vtx)
        return libstinger_core['stinger_vweight_increment'](self.s, ctypes.c_int64(vtx), ctypes.c_int64(vweight))

    def edges_of(self, vtx):
        if isinstance(vtx, str):
            vtx_name = vtx
            vtx = self.get_mapping(vtx)

            deg = self.outdegree(vtx)

            outlen = (ctypes.c_int64 * 1)()
            arr_type = ctypes.c_int64 * deg

            source = [vtx_name] * deg
            neighbor = arr_type()
            weight = arr_type()
            timefirst = arr_type()
            timerecent = arr_type()
            etype = arr_type()

            libstinger_core['stinger_gather_successors'](self.s, ctypes.c_int64(vtx),
                                                         outlen, neighbor, weight, timefirst, timerecent, etype,
                                                         ctypes.c_int64(deg))

            neighbor = [self.get_name(v) for v in neighbor]

            max_etypes = self.num_etypes()
            etype = [self.get_etype_name(t) if t < max_etypes else t for t in etype]

            return zip(etype, source, neighbor, weight, timefirst, timerecent)
        else:
            deg = self.outdegree(vtx)

            outlen = (ctypes.c_int64 * 1)()
            arr_type = ctypes.c_int64 * deg

            source = [vtx] * deg
            neighbor = arr_type()
            weight = arr_type()
            timefirst = arr_type()
            timerecent = arr_type()
            etype = arr_type()

            libstinger_core['stinger_gather_successors'](self.s, ctypes.c_int64(vtx),
                                                         outlen, neighbor, weight, timefirst, timerecent, etype,
                                                         ctypes.c_int64(deg))

            return zip(etype, source, neighbor, weight, timefirst, timerecent)
