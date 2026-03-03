from defind.decoding.registry import EventRegistryProvider, add_event_spec, add_many, make_registry
from defind.decoding.specs import DataFieldSpec, EventSpec, ProjectionRefs, TopicFieldSpec


def _spec(topic0: str, name: str) -> EventSpec:
    return EventSpec(
        topic0=topic0,
        name=name,
        topic_fields=[TopicFieldSpec("user", 1, "address")],
        data_fields=[DataFieldSpec("amount", 0, "uint256")],
        projection={
            "user": ProjectionRefs.TopicRef(name="user"),
            "amount": ProjectionRefs.DataRef(name="amount"),
        },
    )


def test_make_registry_returns_empty_dict() -> None:
    assert make_registry() == {}


def test_add_event_spec_and_add_many_lowercase_keys() -> None:
    reg = make_registry()
    add_event_spec(reg, _spec("0xABC", "E1"))
    add_many(reg, [_spec("0xDEF", "E2")])

    assert set(reg.keys()) == {"0xabc", "0xdef"}
    assert reg["0xabc"].name == "E1"
    assert reg["0xdef"].name == "E2"


def test_event_registry_provider_returns_same_registry() -> None:
    reg = {"0xabc": _spec("0xabc", "E1")}
    provider = EventRegistryProvider(reg)
    assert provider.get_registry() is reg
