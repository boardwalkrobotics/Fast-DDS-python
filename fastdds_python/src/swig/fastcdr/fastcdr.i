%{
#include <fastcdr/Cdr.h>
#include <fastcdr/FastBuffer.h>
#include <fastcdr/exceptions/Exception.h>
#include <fastdds/rtps/common/SerializedPayload.h>
%}

%include <std_string.i>
%include <std_vector.i>
%include <stdint.i>
%include "fastcdr/Cdr.h"
%include "fastcdr/FastBuffer.h"
%include "fastcdr/exceptions/Exception.h"
%include "fastdds/rtps/common/SerializedPayload.h"

%template(VectorUint8) std::vector<uint8_t>;

%extend eprosima::fastcdr::Cdr {
    void serialize_string(const std::string& str) {
    $self->serialize(str);
    }
    void deserialize_string(std::string& str) {
        $self->deserialize(str);
    }
}

%extend eprosima::fastrtps::rtps::SerializedPayload_t {
    std::vector<uint8_t> get_data() {
    return std::vector<uint8_t>($self->data, $self->data + $self->length);
    }

    void set_data(const std::vector<uint8_t>& data) {
        $self->length = data.size();
        std::copy(data.begin(), data.end(), $self->data);
    }
}

