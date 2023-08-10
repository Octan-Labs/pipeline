from web3.module import (
    Module,
)
from web3.method import (
    Method,
    default_root_munger,
)
from typing import (
    Callable,
    List,
)
from web3.types import (
    BlockIdentifier,
    ParityBlockTrace,
    ParityFilterParams,
    ParityFilterTrace,
)
from web3.types import (
    RPCEndpoint,
)

class ArbTraceMethod:
    trace_block = RPCEndpoint('arbtrace_block')
    trace_filter = RPCEndpoint('arbtrace_filter')

class ArbTrace(Module):
    trace_block: Method[Callable[[BlockIdentifier], List[ParityBlockTrace]]] = Method(
        ArbTraceMethod.trace_block,
        mungers=[default_root_munger],
    )

    trace_filter: Method[Callable[[ParityFilterParams], List[ParityFilterTrace]]] = Method(
        ArbTraceMethod.trace_filter,
        mungers=[default_root_munger],
    )