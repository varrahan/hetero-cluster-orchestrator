#!/usr/bin/env python3
import ctypes


cuda = ctypes.CDLL("libcuda.so.1")


def check(name, *args):
    result = getattr(cuda, name)(*args)
    if result:
        raise RuntimeError(f"{name} failed with CUDA error {result}")


device = ctypes.c_int()
context = ctypes.c_void_p()
module = ctypes.c_void_p()
function = ctypes.c_void_p()
device_value = ctypes.c_uint64()
value = ctypes.c_uint32(41)

check("cuInit", 0)
check("cuDeviceGet", ctypes.byref(device), 0)
check("cuCtxCreate_v2", ctypes.byref(context), 0, device)
try:
    ptx = b"""
.version 7.0
.target sm_50
.address_size 64
.visible .entry add_one(.param .u64 input) {
    .reg .b32 value;
    .reg .b64 address;
    ld.param.u64 address, [input];
    ld.global.u32 value, [address];
    add.u32 value, value, 1;
    st.global.u32 [address], value;
    ret;
}
"""
    check("cuModuleLoadData", ctypes.byref(module), ctypes.c_char_p(ptx))
    check("cuModuleGetFunction", ctypes.byref(function), module, b"add_one")
    check("cuMemAlloc_v2", ctypes.byref(device_value), ctypes.sizeof(value))
    try:
        check("cuMemcpyHtoD_v2", device_value, ctypes.byref(value), ctypes.sizeof(value))
        argument = ctypes.c_uint64(device_value.value)
        arguments = (ctypes.c_void_p * 1)(ctypes.cast(ctypes.byref(argument), ctypes.c_void_p))
        check("cuLaunchKernel", function, 1, 1, 1, 1, 1, 1, 0, None, arguments, None)
        check("cuCtxSynchronize")
        check("cuMemcpyDtoH_v2", ctypes.byref(value), device_value, ctypes.sizeof(value))
        assert value.value == 42
    finally:
        check("cuMemFree_v2", device_value)
finally:
    if module:
        check("cuModuleUnload", module)
    check("cuCtxDestroy_v2", context)
