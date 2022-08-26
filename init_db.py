from tools import db


def init_db():
    from .models.tests import UIPerformanceTest
    from .models.reports import UIReport
    from .models.results import UIResult
    from .models.thresholds import UIThreshold
    db.Base.metadata.create_all(bind=db.engine)

