from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from config.settings import END_POINT_TRACING

trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)


otlp_exporter = OTLPSpanExporter(endpoint=END_POINT_TRACING)
span_processor = SimpleSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

def start_trace(operation_name):
    return tracer.start_span(operation_name)
