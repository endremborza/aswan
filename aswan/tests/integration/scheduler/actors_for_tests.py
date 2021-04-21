from aswan.scheduler import ActorFrameBase


class NullActor(ActorFrameBase):
    def consume(self, next_task):
        return


class AddActor(ActorFrameBase):
    def consume(self, next_task):
        return next_task + 1


class OneEvenOneOddActor(ActorFrameBase):
    # TODO
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._odd_done = 0
        self._even_done = 0

    def consume(self, next_task):
        if next_task % 2 == 0:
            self._even_done += 1
        else:
            self._odd_done += 1
        return next_task + 1
