"""Install business-only fixtures over an exported NATSEvents C++ service.

The generated graph, config, makers and transport wiring are never patched.
"""
import pathlib
import sys

service = pathlib.Path(sys.argv[1])
functions = service / 'internal/functions'
types = service / 'internal/types'
(types / 'event.hpp').write_text('''#pragma once
#include <string>
namespace nats_events::event_processor::types {
struct Event final { std::string id; std::string value; };
}
''')
(types / 'publication_outcome.hpp').write_text('''#pragma once
#include <string>
namespace nats_events::event_processor::types {
struct PublicationOutcome final { std::string id; bool accepted{}; };
}
''')
for protocol in ('core', 'durable'):
    path = functions / f'receive_{protocol}_event_source.hpp'
    code = path.read_text()
    begin = code.index('  void consumeMessage(')
    end = code.index('\n  // endRequest', begin)
    code = code[:begin] + '''  void consumeMessage(servicelib::MessageContext context, auto& stream, State&,
      const servicelib::datasource::nats::ConsumerMessage& message, auto result) const {
    const std::string value{message.data()};
    result.setResultCallback(value, [message,result](servicelib::MessageContext,auto&,State&,const types::PublicationOutcome& outcome) mutable {
      if (message.jetStream()) {
        if (outcome.accepted) message.doubleAck(); else message.nak();
      }
      result.done(); return true;
    });
    stream.collect(std::move(context),types::Event{value,value});
  }
  std::string getMessageId(servicelib::MessageContext,auto&,State&,const types::PublicationOutcome& outcome) const {return outcome.id;}
''' + code[end:]
    path.write_text(code)
    path = functions / f'publish_{protocol}_event_sink.hpp'
    code = path.read_text()
    begin = code.index('  void consumeMessage(')
    end = code.index('\n  // endRequest', begin)
    code = code[:begin] + '''  void consumeMessage(servicelib::MessageContext,auto&,State&,const types::Event& value,
      servicelib::datasink::nats::SinkMessage<types::PublicationOutcome>& message) const {
    message.value=value.value;
    message.send([id=value.id](const servicelib::nats::DeliveryResult& result) {
      return types::PublicationOutcome{id,result.accepted && !result.error};
    });
  }
''' + code[end:]
    path.write_text(code)
path = functions / 'normalize_event.hpp'
code = path.read_text().replace('#pragma once', '#pragma once\n#include <algorithm>\n#include <cctype>')
code = code.replace('    throw std::logic_error("NormalizeEvent is not implemented");', '''    auto normalized=value;
    std::transform(normalized.value.begin(),normalized.value.end(),normalized.value.begin(),[](unsigned char c){return std::toupper(c);});
    out(std::move(context),std::move(normalized));''')
path.write_text(code)
