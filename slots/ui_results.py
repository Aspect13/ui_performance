from pylon.core.tools import web, log  # pylint: disable=E0611,E0401
from tools import auth, theme  # pylint: disable=E0401


class Slot:  # pylint: disable=E1101,R0903
    @web.slot('ui_results_content')
    def content(self, context, slot, payload):
        log.info('slot: [%s] || payload: [%s]', slot, payload)
        log.info('payload request args: [%s]', payload.request.args)
        result_id = payload.request.args.get('result_id')
        if result_id:
            test_data = context.rpc_manager.call.ui_results_or_404(result_id)

            with context.app.app_context():
                return self.descriptor.render_template(
                    'results/content.html',
                    test_data=test_data
                )
        return theme.empty_content

    @web.slot('ui_results_scripts')
    def scripts(self, context, slot, payload):
        from pylon.core.tools import log
        log.info('slot: [%s], payload: %s', slot, payload)
        with context.app.app_context():
            return self.descriptor.render_template(
                'results/scripts.html'
            )

    @web.slot('ui_results_styles')
    def styles(self, context, slot, payload):
        from pylon.core.tools import log
        log.info('slot: [%s], payload: %s', slot, payload)
        with context.app.app_context():
            return self.descriptor.render_template(
                'results/styles.html',
            )
