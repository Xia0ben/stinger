import ctypes
import os
from stinger_net import StingerEdgeUpdate


# You must set the STINGER_LIB_PATH environment variable to be able to use this wrapper. You can do so by executing:
# export STINGER_LIB_PATH=<REPLACE-BY-ABSOLUTE-PATH-TO-STINGER-FOLDER>/stinger/build/lib/
if os.getenv('STINGER_LIB_PATH'):
    libstinger_alg = ctypes.cdll.LoadLibrary(os.getenv('STINGER_LIB_PATH') + '/libstinger_alg.so')
else:
    libstinger_alg = ctypes.cdll.LoadLibrary('libstinger_core.so')


# region Wrapper for Streamed Connected Components
class StingerConnectedComponentsStats(ctypes.Structure):
    """
    Stinger Connected Components stats data structure
    """
    _fields_ = [
        ("bfs_deletes_in_tree", ctypes.c_uint64),
        ("bfs_inserts_in_tree_as_parents", ctypes.c_uint64),
        ("bfs_inserts_in_tree_as_neighbors", ctypes.c_uint64),
        ("bfs_inserts_in_tree_as_replacement", ctypes.c_uint64),
        ("bfs_inserts_bridged", ctypes.c_uint64),
        ("bfs_unsafe_deletes", ctypes.c_uint64)]


class StingerSccInternal(ctypes.Structure):
    """
    Stinger Streamed Connected Components Internal data structure
    """
    _fields_ = [
        ("bfsDataPtr", ctypes.POINTER(ctypes.c_int64)),
        ("queue", ctypes.POINTER(ctypes.c_int64)),
        ("level", ctypes.POINTER(ctypes.c_int64)),
        ("found", ctypes.POINTER(ctypes.c_int64)),
        ("same_level_queue", ctypes.POINTER(ctypes.c_int64)),
        ("parentsDataPtr", ctypes.POINTER(ctypes.c_int64)),
        ("parentArray", ctypes.POINTER(ctypes.c_int64)),
        ("parentCounter", ctypes.POINTER(ctypes.c_int64)),
        ("bfs_components", ctypes.POINTER(ctypes.c_int64)),
        ("bfs_component_sizes", ctypes.POINTER(ctypes.c_int64)),
        ("nv", ctypes.c_uint64),
        ("parentsPerVertex", ctypes.c_uint64),
        ("initCCCount", ctypes.c_uint64)]


def stinger_scc_initialize_internals(s, nv, scc_internal, parents_per_vertex=4):
    """
    Needs to be called once before the updates are processed.
    This function uses a "static graph" algorithm to initialize the data
    structure that is used for the streaming updates.
    Recommended default for (parentsPerVertex==4).
    :param s: stinger data structure
    :type s: Stinger
    :param nv:
    :type nv: int
    :param scc_internal:
    :type scc_internal: StingerSccInternal
    :param parents_per_vertex:
    :type parents_per_vertex: int
    """
    libstinger_alg['stinger_scc_initialize_internals'](
        s.raw(), ctypes.c_int64(nv), scc_internal, ctypes.c_int64(parents_per_vertex))


def stinger_scc_reset_stats(stats):
    """
    Should be called before each batch update.
    :param stats:
    :type stats: StingerConnectedComponentsStats
    """
    libstinger_alg['stinger_scc_reset_stats'](stats)


def stinger_scc_insertion(s, nv,  scc_internal, stats, batch, batch_size):
    """

    :param s:
    :type s: Stinger
    :param nv:
    :type nv: int
    :param scc_internal:
    :type scc_internal: StingerSccInternal
    :param stats:
    :type stats: StingerConnectedComponentsStats
    :param batch:
    :type batch: StingerEdgeUpdate
    :param batch_size:
    :type batch_size: int
    :return:
    :rtype: int
    """
    return libstinger_alg['stinger_scc_insertion'](
        s.raw(), ctypes.c_int64(nv), scc_internal, stats, batch, ctypes.c_int64(batch_size)).value


def stinger_scc_deletion(s, nv,  scc_internal, stats, batch, batch_size):
    """

    :param s:
    :type s: Stinger
    :param nv:
    :type nv: int
    :param scc_internal:
    :type scc_internal: StingerSccInternal
    :param stats:
    :type stats: StingerConnectedComponentsStats
    :param batch:
    :type batch: StingerEdgeUpdate or ctypes.Array(StingerEdgeUpdate)
    :param batch_size:
    :type batch_size: int
    :return:
    :rtype: int
    """
    return libstinger_alg['stinger_scc_deletion'](
        s.raw(), ctypes.c_int64(nv), scc_internal, stats, batch, ctypes.c_int64(batch_size)).value


def stinger_scc_copy_component_array(scc_internal, dest_array):
    """

    :param scc_internal:
    :type scc_internal: StingerSccInternal
    :param dest_array:
    :type dest_array: ctypes.POINTER(ctypes.c_void_p)
    """
    libstinger_alg['stinger_scc_copy_component_array'](scc_internal, dest_array)


def stinger_scc_release_internals(scc_internal):
    """

    :param scc_internal:
    :type scc_internal: StingerSccInternal
    """
    libstinger_alg['stinger_scc_release_internals'](scc_internal)


def stinger_scc_get_components(scc_internal):
    """
    TODO Convert return type to list(int)
    :param scc_internal:
    :type scc_internal: StingerSccInternal
    :return:
    :rtype: ctypes.POINTER(ctypes.c_int64)
    """
    return libstinger_alg['stinger_scc_get_components'](scc_internal)


def stinger_scc_print_insert_stats(stats):
    """

    :param stats:
    :type stats: StingerConnectedComponentsStats
    """
    libstinger_alg['stinger_scc_print_insert_stats'](stats)


def stinger_scc_print_delete_stats(stats):
    """

    :param stats:
    :type stats: StingerConnectedComponentsStats
    """
    libstinger_alg['stinger_scc_print_delete_stats'](stats)

# endregion
