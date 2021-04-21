import hashlib
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import List, Optional, Type, Union


class LimitExceeded(Exception):
    pass


class LimitKinds:

    COUNT = "count"
    NUNIQUE = "nunique"
    SUM = "sum"
    MAX_VALUE_COUNT = "max_value_count"


limit_kind_checks = {
    LimitKinds.SUM: lambda limit, att_values: (limit < sum(att_values)),
    LimitKinds.COUNT: lambda limit, att_values: (limit < len(att_values)),
    LimitKinds.NUNIQUE: lambda limit, att_values: (
        limit < len(set(att_values))
    ),
    LimitKinds.MAX_VALUE_COUNT: lambda limit, att_values: (
        limit < max(Counter(att_values).values())
    ),
}


def get_resource_name(resource: "Resource") -> str:
    param_str = ", ".join(f"{k}={v}" for k, v in resource.__dict__.items())
    return f"{type(resource).__name__}({param_str})"


class Resource:
    actor_resource = True  # otherwise task resource

    def __repr__(self):
        return get_resource_name(self)

    def __hash__(self):
        return self.__repr__().__hash__()

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()


class ResourceBundle:
    def __init__(self, resource_list: Optional[List[Resource]] = None):
        self._resource_list: List[Resource] = resource_list or []
        self.full_dic = self._to_dict(actor_specific=False)
        self.actor_dic = self._to_dict(actor_specific=True)
        self.key = self._create_key()

    def __eq__(self, other):
        assert isinstance(other, ResourceBundle)
        return self.full_dic == other.full_dic

    def __le__(self, other):
        assert isinstance(other, ResourceBundle)
        return all(
            [
                v <= other.full_dic.get(k, -float("inf"))
                for k, v in self.full_dic.items()
            ]
        )

    def __ge__(self, other):
        return other <= self

    def __lt__(self, other):
        return not (self == other) & (self <= other)

    def __gt__(self, other):
        return not (self == other) & (self >= other)

    def __mul__(self, other: int):
        return ResourceBundle(self._resource_list * other)

    def __add__(self, other: "ResourceBundle"):
        return ResourceBundle([*self.resource_list, *other.resource_list])

    def __sub__(self, other: "ResourceBundle"):
        new_dic = self.full_dic.copy()
        for k, v in other.full_dic.items():
            new_val = new_dic.get(k, 0) - v
            assert new_val >= 0
            new_dic[k] = new_val

        return self._from_dic(new_dic)

    def __repr__(self):
        return str(dict(self.full_dic))

    @property
    def resource_list(self):
        return self._resource_list

    @property
    def actor_subbundle(self):
        return ResourceBundle(
            [r for r in self._resource_list if r.actor_resource]
        )

    def _to_dict(self, actor_specific=True) -> dict:
        res_dic = defaultdict(lambda: 0)
        for res in self._resource_list:
            if (not res.actor_resource) and actor_specific:
                continue
            res_dic[res] += 1
        return res_dic

    def _create_key(self):
        ctxt = "-".join(
            map(str, sorted(self.full_dic.items(), key=lambda x: str(x[0])))
        )
        s = sum(self.full_dic.values())
        return (
            f'{s:07d}-{hashlib.sha256(ctxt.encode("utf-8")).hexdigest()[:10]}'
        )

    @classmethod
    def _from_dic(cls, full_dic: dict):
        res_list = []
        for k, v in full_dic.items():
            res_list += [k] * v
        return cls(res_list)


@dataclass
class ResourceLimit:
    resource: Type[Resource]
    global_limit: int = 0
    target_attribute: Optional[str] = None
    limit_kind: str = LimitKinds.COUNT


class ResourceLimitSet:
    def __init__(self, resource_limits: Optional[List[ResourceLimit]] = None):
        self._resource_limits = resource_limits or []
        self._limit_dict = defaultdict(lambda: defaultdict(dict))
        for reslim in self._resource_limits:
            local_limit_dic = self._limit_dict[reslim.resource][
                reslim.target_attribute
            ]
            assert reslim.limit_kind not in local_limit_dic.keys()
            local_limit_dic[reslim.limit_kind] = reslim.global_limit

    def satisfied(self, resource_bundle: ResourceBundle):
        _usage_dict = defaultdict(lambda: defaultdict(list))
        for resource in resource_bundle.resource_list:
            try:
                self._add_to_dict(resource, _usage_dict)
            except LimitExceeded:
                return False
        return True

    def to_dict(self, actor_specific=True) -> dict:
        res_dic = {}
        for lim in self._resource_limits:
            if (not lim.resource.actor_resource) and actor_specific:
                continue
            if lim.global_limit:
                res_dic[lim.resource.__name__] = lim.global_limit
        return res_dic

    def _add_to_dict(self, resource: Resource, usage_dict: dict):
        res_kls = type(resource)
        for att_name, att_limit_kinds in self._limit_dict.get(
            res_kls, {}
        ).items():
            att_val = 1 if att_name is None else getattr(resource, att_name)

            used_att_values = usage_dict[res_kls][att_name]
            used_att_values.append(att_val)
            for limit_kind, limit_kind_value in att_limit_kinds.items():
                self._evaluate_limit_kind(
                    limit_kind, limit_kind_value, used_att_values
                )

    @staticmethod
    def _evaluate_limit_kind(
        limit_kind: str,
        limit_kind_value: Union[float, int],
        used_att_values: list,
    ):
        if limit_kind_checks[limit_kind](limit_kind_value, used_att_values):
            raise LimitExceeded
