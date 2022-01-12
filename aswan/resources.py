from enum import Enum

from atqo import Capability


class REnum(Enum):

    mCPU = 0
    DISPLAY = 1


class Caps:
    simple = Capability({REnum.mCPU: 250})
    eager_browser = Capability({REnum.mCPU: 750})
    normal_browser = Capability({REnum.mCPU: 750})
    display = Capability({REnum.DISPLAY: 1, REnum.mCPU: 250})
