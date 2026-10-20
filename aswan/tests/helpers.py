from functools import partial

from sqlalchemy.orm import Session

from aswan.constants import Statuses
from aswan.depot import AswanDepot
from aswan.models import CollEvent, RegEvent, SourceUrl

get_cev = partial(
    CollEvent,
    url="link-1",
    handler="A",
    status=Statuses.PROCESSED,
    timestamp=1000,
    output_file="of1",
)


def seed_run(depot: AswanDepot, **cev_kwargs) -> None:
    """Commit one run registering and collecting one url; the depot takes more."""
    cev = get_cev(**cev_kwargs)
    depot.current.integrate_events([RegEvent(cev.url, cev.handler), cev])
    depot.save_current()
    depot.current.purge()
    depot.init_w_complete()


def statuses(depot: AswanDepot) -> dict[tuple[str, str], str]:
    """(handler, url) -> status as of the committed runs, ignoring the current run."""
    depot.current.purge()
    depot.init_w_complete()
    with Session(depot.current.engine) as session:
        return {(s.handler, s.url): s.current_status for s in session.query(SourceUrl)}
