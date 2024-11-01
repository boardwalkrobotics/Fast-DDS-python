#include <fastdds/dds/core/policy/QosPolicies.hpp>
#include <fastdds/dds/core/status/PublicationMatchedStatus.hpp>
#include <fastdds/dds/core/status/StatusMask.hpp>
#include <fastdds/dds/core/status/SubscriptionMatchedStatus.hpp>
#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/DomainParticipantListener.hpp>
#include <fastdds/dds/publisher/DataWriter.hpp>
#include <fastdds/dds/publisher/DataWriterListener.hpp>
#include <fastdds/dds/publisher/Publisher.hpp>
#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/subscriber/DataReader.hpp>
#include <fastdds/dds/subscriber/SampleInfo.hpp>
#include <fastdds/dds/subscriber/Subscriber.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <fastdds/dds/topic/Topic.hpp>
#include <fastdds/dds/topic/TopicDataType.hpp>
#include <fastdds/dds/topic/TypeSupport.hpp>
#include <fastdds/dds/topic/qos/TopicQos.hpp>
#include <fastdds/rtps/common/SerializedPayload.h>
#include <fastdds/rtps/history/IPayloadPool.h>
#include <fastdds/rtps/participant/ParticipantDiscoveryInfo.h>
#include <fastdds/rtps/reader/ReaderDiscoveryInfo.h>
#include <fastdds/rtps/writer/WriterDiscoveryInfo.h>
#include <pybind11/operators.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;
using namespace eprosima::fastdds::dds;
using SerializedPayload_t = eprosima::fastrtps::rtps::SerializedPayload_t;
using IPayloadPool = eprosima::fastrtps::rtps::IPayloadPool;

////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////// "Trampoline" Python classes////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

class PyDomainParticipantListener : public DomainParticipantListener {
   public:
    using DomainParticipantListener::DomainParticipantListener;  // Inherit
                                                                 // constructors

    void on_participant_discovery(DomainParticipant* participant,
                                  eprosima::fastrtps::rtps::ParticipantDiscoveryInfo&& info) override {
        PYBIND11_OVERRIDE_PURE(void, DomainParticipantListener, on_participant_discovery, participant, std::ref(info));
    }

    void on_subscriber_discovery(DomainParticipant* participant,
                                 eprosima::fastrtps::rtps::ReaderDiscoveryInfo&& info) override {
        PYBIND11_OVERRIDE_PURE(void, DomainParticipantListener, on_subscriber_discovery, participant, std::ref(info));
    }

    void on_publisher_discovery(DomainParticipant* participant,
                                eprosima::fastrtps::rtps::WriterDiscoveryInfo&& info) override {
        PYBIND11_OVERRIDE_PURE(void, DomainParticipantListener, on_publisher_discovery, participant, std::ref(info));
    }

    void on_type_discovery(DomainParticipant* participant,
                           const eprosima::fastrtps::rtps::SampleIdentity& request_sample_id,
                           const eprosima::fastrtps::string_255& topic,
                           const eprosima::fastrtps::types::TypeIdentifier* identifier,
                           const eprosima::fastrtps::types::TypeObject* object,
                           eprosima::fastrtps::types::DynamicType_ptr dyn_type) override {
        PYBIND11_OVERLOAD(void, DomainParticipantListener, on_type_discovery, participant, request_sample_id, topic,
                          identifier, object, dyn_type);
    }

    void on_type_dependencies_reply(DomainParticipant* participant,
                                    const eprosima::fastrtps::rtps::SampleIdentity& request_sample_id,
                                    const eprosima::fastrtps::types::TypeIdentifierWithSizeSeq& dependencies) override {
        PYBIND11_OVERLOAD(void, DomainParticipantListener, on_type_dependencies_reply, participant, request_sample_id,
                          dependencies);
    }

    void on_type_information_received(DomainParticipant* participant, const eprosima::fastrtps::string_255 topic_name,
                                      const eprosima::fastrtps::string_255 type_name,
                                      const eprosima::fastrtps::types::TypeInformation& type_information) override {
        PYBIND11_OVERLOAD(void, DomainParticipantListener, on_type_information_received, participant, topic_name,
                          type_name, type_information);
    }
};

class PyTopicDataType : public TopicDataType {
   private:
    py::object python_type;
    py::object self_py;

   public:
    using TopicDataType::TopicDataType;
    PyTopicDataType(py::object py_type) : python_type(py_type), TopicDataType() {
        // setName(py::str(python_type.attr("__name__")).cast<std::string>().c_str());
        // m_typeSize = 0;
        // m_isGetKeyDefined = false;

        // py::module cdr_module = py::module::import("pypubsub.cdr");
        // cdr_class = cdr_module.attr("CDR");
        self_py = py::cast(this);
    }

    bool serialize(void* data, SerializedPayload_t* payload) override {
        // Due to FastDDS using void* for passing around data, we need to cast
        // the PyCapsule to the original python object
        // PYBIND11_OVERRIDE_PURE(bool, TopicDataType, serialize, data, payload);

        py::gil_scoped_acquire acquire;
        py::object* py_data = static_cast<py::object*>(data);

        py::bytes result = _serialize(*py_data);

        std::cout << result << std::endl;
        char* buffer;
        ssize_t length;
        if (PyBytes_AsStringAndSize(result.ptr(), &buffer, &length) != -1) {
            // payload->reserve(static_cast<uint32_t>(length));
            std::memcpy(payload->data, buffer, length);
            payload->length = static_cast<uint32_t>(length);
            return true;
        } else {
            throw py::error_already_set();
        }

        return false;
    }

    bool deserialize(SerializedPayload_t* payload, void* data) override {
        PYBIND11_OVERRIDE_PURE(bool, TopicDataType, deserialize, payload, data);
    }

    std::function<uint32_t()> getSerializedSizeProvider(void* data) override {
        // PYBIND11_OVERRIDE_PURE(std::function<uint32_t()>, TopicDataType,
        // getSerializedSizeProvider, data);
        try {
            py::object py_size = _get_serialized_size(data);
            uint32_t size = py::cast<uint32_t>(py_size);
            return [size]() -> uint32_t { return size; };
        } catch (py::error_already_set& e) {
            throw e;
        }
    }

    void* createData() override {
        PYBIND11_OVERRIDE_PURE(void*, TopicDataType, createData);
    }

    void deleteData(void* data) override {
        PYBIND11_OVERRIDE_PURE(void, TopicDataType, deleteData, data);
    }

    bool getKey(void* data, InstanceHandle_t* ihandle, bool force_md5 = false) override {
        // PYBIND11_OVERRIDE_PURE(bool, TopicDataType, getKey, data, ihandle,
        // force_md5);
        return false;
    }

    bool is_bounded() const override {
        PYBIND11_OVERRIDE_PURE(bool, TopicDataType, is_bounded);
    }

    virtual py::bytes _serialize(py::object data) {
        PYBIND11_OVERRIDE_PURE(py::bytes, PyTopicDataType, _serialize, data);
    }

    virtual py::object _get_serialized_size(void* data) {
        PYBIND11_OVERRIDE_PURE(py::object, PyTopicDataType, _get_serialized_size, data);
    }
    // TypeSupport getTypeSupport() {
    //     return TypeSupport(this);
    // }
};

TypeSupport create_type_support(TopicDataType* topic_data_type) {
    return TypeSupport(topic_data_type);
}

// class PyTypeSupport {
// private:
//     TypeSupport type_support_;
// public:
//     PyTypeSupport() = default;
//     PyTypeSupport(std::shared_ptr<TopicDataType> type) : type_support(type)
//     {}

//     ReturnCode_t register_type(DomainParticipant* participant, const
//     std::string& type_name) {
//         return type_support_.register_type(participant, type_name);
//     }

//     const std::string& get_type_name() const {
//         return type_support_.get_type_name();
//     }

//     bool serialize(const py::object& data, st)
// }

////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////// fastdds_pybind module ////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

PYBIND11_MODULE(fastdds_pybind, m) {
    py::class_<Topic, std::shared_ptr<Topic>>(m, "Topic");

    py::class_<SerializedPayload_t>(m, "SerializedPayload_t")
        .def(py::init<>())
        .def_property(
            "data",
            [](SerializedPayload_t& self) -> py::bytes {
                return py::bytes(reinterpret_cast<char*>(self.data), self.length);
            },
            [](SerializedPayload_t& self, py::bytes bytes) {
                char* buffer;
                ssize_t length;
                if (PyBytes_AsStringAndSize(bytes.ptr(), &buffer, &length) != -1) {
                    self.length = static_cast<uint32_t>(length);
                    self.max_size = self.length;
                    self.reserve(self.length);
                    std::memcpy(self.data, buffer, self.length);
                } else {
                    throw py::error_already_set();
                }
            })
        .def_readwrite("length", &SerializedPayload_t::length)
        .def_readwrite("max_size", &SerializedPayload_t::max_size)
        .def("reserve", &SerializedPayload_t::reserve);

    py::class_<TopicDataType, PyTopicDataType>(m, "TopicDataType")
        .def(py::init<py::object>())
        .def("_serialize", [](PyTopicDataType& self, py::object data) { return self._serialize(data); })
        .def("deserialize", [](TopicDataType& self, SerializedPayload_t* payload,
                               py::object data) { return self.deserialize(payload, data.ptr()); })
        .def("_get_serialized_size",
             [](PyTopicDataType& self, py::object data) { return self._get_serialized_size(data.ptr()); })
        // .def("createData", &TopicDataType::createData)
        // .def("deleteData", [](TopicDataType &self, py::object data)
        //  { self.deleteData(data.ptr()); })
        // .def("getKey", [](TopicDataType &self, py::object data,
        // InstanceHandle_t *ihandle, bool force_md5)
        //      { return self.getKey(data.ptr(), ihandle, force_md5); })
        .def("getName", &TopicDataType::getName)
        .def("setName", &TopicDataType::setName)
        .def("getTypeSupport", [](TopicDataType& self) { return create_type_support(&self); })
        .def_readwrite("m_typeSize", &TopicDataType::m_typeSize)
        .def_readwrite("m_isGetKeyDefined", &TopicDataType::m_isGetKeyDefined);

    py::class_<TypeSupport>(m, "TypeSupport")
        .def(py::init<>())
        .def(py::init<TopicDataType*>())
        .def("register_type",
             py::overload_cast<DomainParticipant*, std::string>(&TypeSupport::register_type, py::const_))
        .def("get_type_name", &TypeSupport::get_type_name)
        .def("serialize", py::overload_cast<void*, SerializedPayload_t*>(&TypeSupport::serialize));

    py::class_<TopicQos>(m, "TopicQos").def(py::init<>());

    py::class_<TopicListener>(m, "TopicListener").def(py::init<>());

    py::class_<Duration_t>(m, "Duration_t")
        .def(py::init<>())
        .def_readwrite("seconds", &Duration_t::seconds)
        .def_readwrite("nanosec", &Duration_t::nanosec);

    py::class_<ReturnCode_t>(m, "ReturnCode_t")
        .def(py::init<>())
        .def(py::init<uint32_t>())
        .def("__eq__", &ReturnCode_t::operator==)
        .def("__ne__", &ReturnCode_t::operator!=)
        .def("__call__", &ReturnCode_t::operator())
        .def("is_not_ok", &ReturnCode_t::operator!)
        .def("__str__",
             [](const ReturnCode_t& rc) {
                 switch (rc()) {
                     case ReturnCode_t::RETCODE_OK:
                         return "RETCODE_OK";
                     case ReturnCode_t::RETCODE_ERROR:
                         return "RETCODE_ERROR";
                     case ReturnCode_t::RETCODE_UNSUPPORTED:
                         return "RETCODE_UNSUPPORTED";
                     case ReturnCode_t::RETCODE_BAD_PARAMETER:
                         return "RETCODE_BAD_PARAMETER";
                     case ReturnCode_t::RETCODE_PRECONDITION_NOT_MET:
                         return "RETCODE_PRECONDITION_NOT_MET";
                     case ReturnCode_t::RETCODE_OUT_OF_RESOURCES:
                         return "RETCODE_OUT_OF_RESOURCES";
                     case ReturnCode_t::RETCODE_NOT_ENABLED:
                         return "RETCODE_NOT_ENABLED";
                     case ReturnCode_t::RETCODE_IMMUTABLE_POLICY:
                         return "RETCODE_IMMUTABLE_POLICY";
                     case ReturnCode_t::RETCODE_INCONSISTENT_POLICY:
                         return "RETCODE_INCONSISTENT_POLICY";
                     case ReturnCode_t::RETCODE_ALREADY_DELETED:
                         return "RETCODE_ALREADY_DELETED";
                     case ReturnCode_t::RETCODE_TIMEOUT:
                         return "RETCODE_TIMEOUT";
                     case ReturnCode_t::RETCODE_NO_DATA:
                         return "RETCODE_NO_DATA";
                     case ReturnCode_t::RETCODE_ILLEGAL_OPERATION:
                         return "RETCODE_ILLEGAL_OPERATION";
                     case ReturnCode_t::RETCODE_NOT_ALLOWED_BY_SECURITY:
                         return "RETCODE_NOT_ALLOWED_BY_SECURITY";
                     default:
                         return "Unknown ReturnCode";
                 }
             })
        .def_property_readonly_static("RETCODE_OK", [](py::object) { return ReturnCode_t::RETCODE_OK; })
        .def_property_readonly_static("RETCODE_ERROR", [](py::object) { return ReturnCode_t::RETCODE_ERROR; })
        .def_property_readonly_static("RETCODE_UNSUPPORTED",
                                      [](py::object) { return ReturnCode_t::RETCODE_UNSUPPORTED; })
        .def_property_readonly_static("RETCODE_BAD_PARAMETER",
                                      [](py::object) { return ReturnCode_t::RETCODE_BAD_PARAMETER; })
        .def_property_readonly_static("RETCODE_PRECONDITION_NOT_MET",
                                      [](py::object) { return ReturnCode_t::RETCODE_PRECONDITION_NOT_MET; })
        .def_property_readonly_static("RETCODE_OUT_OF_RESOURCES",
                                      [](py::object) { return ReturnCode_t::RETCODE_OUT_OF_RESOURCES; })
        .def_property_readonly_static("RETCODE_NOT_ENABLED",
                                      [](py::object) { return ReturnCode_t::RETCODE_NOT_ENABLED; })
        .def_property_readonly_static("RETCODE_IMMUTABLE_POLICY",
                                      [](py::object) { return ReturnCode_t::RETCODE_IMMUTABLE_POLICY; })
        .def_property_readonly_static("RETCODE_INCONSISTENT_POLICY",
                                      [](py::object) { return ReturnCode_t::RETCODE_INCONSISTENT_POLICY; })
        .def_property_readonly_static("RETCODE_ALREADY_DELETED",
                                      [](py::object) { return ReturnCode_t::RETCODE_ALREADY_DELETED; })
        .def_property_readonly_static("RETCODE_TIMEOUT", [](py::object) { return ReturnCode_t::RETCODE_TIMEOUT; })
        .def_property_readonly_static("RETCODE_NO_DATA", [](py::object) { return ReturnCode_t::RETCODE_NO_DATA; })
        .def_property_readonly_static("RETCODE_ILLEGAL_OPERATION",
                                      [](py::object) { return ReturnCode_t::RETCODE_ILLEGAL_OPERATION; })
        .def_property_readonly_static("RETCODE_NOT_ALLOWED_BY_SECURITY",
                                      [](py::object) { return ReturnCode_t::RETCODE_NOT_ALLOWED_BY_SECURITY; });

    // py::enum_<ReturnCode_t::ReturnCodeValue>(m, "ReturnCodeValue")
    //     .value("RETCODE_OK", ReturnCode_t::RETCODE_OK)
    //     .value("RETCODE_ERROR", ReturnCode_t::RETCODE_ERROR)
    //     .value("RETCODE_UNSUPPORTED", ReturnCode_t::RETCODE_UNSUPPORTED)
    //     .value("RETCODE_BAD_PARAMETER", ReturnCode_t::RETCODE_BAD_PARAMETER)
    //     .value("RETCODE_PRECONDITION_NOT_MET",
    //     ReturnCode_t::RETCODE_PRECONDITION_NOT_MET)
    //     .value("RETCODE_OUT_OF_RESOURCES",
    //     ReturnCode_t::RETCODE_OUT_OF_RESOURCES) .value("RETCODE_NOT_ENABLED",
    //     ReturnCode_t::RETCODE_NOT_ENABLED) .value("RETCODE_IMMUTABLE_POLICY",
    //     ReturnCode_t::RETCODE_IMMUTABLE_POLICY)
    //     .value("RETCODE_INCONSISTENT_POLICY",
    //     ReturnCode_t::RETCODE_INCONSISTENT_POLICY)
    //     .value("RETCODE_ALREADY_DELETED",
    //     ReturnCode_t::RETCODE_ALREADY_DELETED) .value("RETCODE_TIMEOUT",
    //     ReturnCode_t::RETCODE_TIMEOUT) .value("RETCODE_NO_DATA",
    //     ReturnCode_t::RETCODE_NO_DATA) .value("RETCODE_ILLEGAL_OPERATION",
    //     ReturnCode_t::RETCODE_ILLEGAL_OPERATION)
    //     .value("RETCODE_NOT_ALLOWED_BY_SECURITY",
    //     ReturnCode_t::RETCODE_NOT_ALLOWED_BY_SECURITY);

    // py::class_<PublicationMatchedStatus>(m, "PublicationMatchedStatus")
    //     .def(py::init<>())
    //     .def_readwrite("total_count", &PublicationMatchedStatus::total_count)
    //     .def_readwrite("total_count_change",
    //     &PublicationMatchedStatus::total_count_change)
    //     .def_readwrite("current_count",
    //     &PublicationMatchedStatus::current_count)
    //     .def_readwrite("current_count_change",
    //     &PublicationMatchedStatus::current_count_change);

    // py::class_<SubscriptionMatchedStatus>(m, "SubscriptionMatchedStatus")
    //     .def(py::init<>())
    //     .def_readwrite("total_count", &SubscriptionMatchedStatus::total_count)
    //     .def_readwrite("total_count_change",
    //     &SubscriptionMatchedStatus::total_count_change)
    //     .def_readwrite("current_count",
    //     &SubscriptionMatchedStatus::current_count)
    //     .def_readwrite("current_count_change",
    //     &SubscriptionMatchedStatus::current_count_change);

    py::class_<StatusMask>(m, "StatusMask")
        .def(py::init<>())
        .def(py::init<uint32_t>())
        .def("is_active", &StatusMask::is_active)
        // .def("get_function", &StatusMask::get_function)
        .def_static("all", &StatusMask::all)
        .def_static("none", &StatusMask::none)
        .def_static("inconsistent_topic", &StatusMask::inconsistent_topic)
        .def_static("offered_deadline_missed", &StatusMask::offered_deadline_missed)
        .def_static("requested_deadline_missed", &StatusMask::requested_deadline_missed)
        .def_static("offered_incompatible_qos", &StatusMask::offered_incompatible_qos)
        .def_static("requested_incompatible_qos", &StatusMask::requested_incompatible_qos)
        .def_static("sample_lost", &StatusMask::sample_lost)
        .def_static("sample_rejected", &StatusMask::sample_rejected)
        .def_static("data_on_readers", &StatusMask::data_on_readers)
        .def_static("data_available", &StatusMask::data_available)
        .def_static("liveliness_lost", &StatusMask::liveliness_lost)
        .def_static("liveliness_changed", &StatusMask::liveliness_changed)
        .def_static("publication_matched", &StatusMask::publication_matched)
        .def_static("subscription_matched", &StatusMask::subscription_matched)
        // .def(py::self | py::self)
        // .def(py::self & py::self)
        // .def(py::self ^ py::self)
        // .def(py::self << py::self)
        // .def(py::self >> py::self)
        .def(~py::self);

    py::class_<IPayloadPool, std::shared_ptr<IPayloadPool>>(m, "IPayloadPool");

    py::class_<DataWriterListener>(m, "DataWriterListener").def(py::init<>());

    py::class_<DataWriterQos>(m, "DataWriterQos").def(py::init<>());
    // .def_readwrite("history", &DataWriterQos::history);

    py::enum_<eprosima::fastdds::dds::HistoryQosPolicyKind>(m, "HistoryQosPolicyKind")
        .value("KEEP_LAST", eprosima::fastdds::dds::HistoryQosPolicyKind::KEEP_LAST_HISTORY_QOS)
        .value("KEEP_ALL", eprosima::fastdds::dds::HistoryQosPolicyKind::KEEP_ALL_HISTORY_QOS);

    py::class_<HistoryQosPolicy>(m, "HistoryQosPolicy").def(py::init<>());
    // .def_property("kind", [](const HistoryQosPolicy &qos)
    //               { return qos.kind(); }, [](HistoryQosPolicy &qos, const
    //               eprosima::fastdds::dds::HistoryQosPolicyKind &kind) {
    //               qos.kind(kind); })
    // .def_property("depth", [](const HistoryQosPolicy &qos)
    //               { return qos.depth(); }, [](HistoryQosPolicy &qos, int32_t
    //               depth) { qos.depth(depth); });

    py::class_<PublisherListener>(m, "PublisherListener").def(py::init<>());

    py::class_<PublisherQos>(m, "PublisherQos")
        .def(py::init<>())
        .def_property(
            "presentation", [](const PublisherQos& qos) { return qos.presentation(); },
            [](PublisherQos& qos, const eprosima::fastdds::dds::PresentationQosPolicy& presentation) {
                qos.presentation(presentation);
            })
        .def_property(
            "partition", [](const PublisherQos& qos) { return qos.partition(); },
            [](PublisherQos& qos, const eprosima::fastdds::dds::PartitionQosPolicy& partition) {
                qos.partition(partition);
            })
        .def_property(
            "group_data", [](const PublisherQos& qos) { return qos.group_data().data_vec(); },
            [](PublisherQos& qos, const std::vector<eprosima::fastrtps::rtps::octet>& data) {
                qos.group_data().data_vec(data);
            })
        .def_property(
            "entity_factory", [](const PublisherQos& qos) { return qos.entity_factory(); },
            [](PublisherQos& qos, const eprosima::fastdds::dds::EntityFactoryQosPolicy& entity_factory) {
                qos.entity_factory(entity_factory);
            });

    // DataWriter binding
    py::class_<DataWriter>(m, "DataWriter")
        .def("get_topic", &DataWriter::get_topic, py::return_value_policy::reference)
        // .def("get_qos", py::overload_cast<>(&DataWriter::get_qos))
        .def("get_qos", py::overload_cast<DataWriterQos&>(&DataWriter::get_qos, py::const_))
        // .def("get_qos", &DataWriter::get_qos)
        .def("set_qos", &DataWriter::set_qos)
        .def("get_listener", &DataWriter::get_listener, py::return_value_policy::reference)
        .def("set_listener", py::overload_cast<DataWriterListener*, const StatusMask&>(&DataWriter::set_listener))
        .def("assert_liveliness", &DataWriter::assert_liveliness)
        .def("write", [](DataWriter& self, py::object& obj) {
            // void *data = obj.cast<void *>();
            py::object* data_ptr = new py::object(obj);
            bool res = self.write(data_ptr);
            delete data_ptr;
            return res;
        });

    py::class_<Publisher>(m, "Publisher")
        .def(
            "create_datawriter",
            [](Publisher& self, Topic& topic, const DataWriterQos& qos, DataWriterListener* listener,
               const StatusMask& mask) { return self.create_datawriter(&topic, qos, listener, mask); },
            py::arg("topic"), py::arg("qos"), py::arg("listener") = nullptr, py::arg("mask") = StatusMask::all())
        .def("get_default_datawriter_qos",
             static_cast<const DataWriterQos& (Publisher::*)() const>(&Publisher::get_default_datawriter_qos));

    // py::class_<DataReader>(m, "DataReader")
    //     .def("read_next_sample", &DataReader::read_next_sample);

    py::class_<DataReaderQos>(m, "DataReaderQos").def(py::init<>());

    // py::class_<SampleInfo>(m, "SampleInfo")
    //     .def(py::init<>())
    //     .def_readwrite("sample_state", &SampleInfo::sample_state)
    //     .def_readwrite("view_state", &SampleInfo::view_state)
    //     .def_readwrite("instance_state", &SampleInfo::instance_state)
    //     .def_readwrite("source_timestamp", &SampleInfo::source_timestamp)
    //     .def_readwrite("instance_handle", &SampleInfo::instance_handle);

    py::class_<SubscriberQos>(m, "SubscriberQos")
        .def(py::init<>())
        .def_property(
            "presentation", [](const SubscriberQos& qos) { return qos.presentation(); },
            [](SubscriberQos& qos, const eprosima::fastdds::dds::PresentationQosPolicy& presentation) {
                qos.presentation(presentation);
            })
        .def_property(
            "partition", [](const SubscriberQos& qos) { return qos.partition(); },
            [](SubscriberQos& qos, const eprosima::fastdds::dds::PartitionQosPolicy& partition) {
                qos.partition(partition);
            })
        .def_property(
            "group_data", [](const SubscriberQos& qos) { return qos.group_data().data_vec(); },
            [](SubscriberQos& qos, const std::vector<eprosima::fastrtps::rtps::octet>& data) {
                qos.group_data().data_vec(data);
            })
        .def_property(
            "entity_factory", [](const SubscriberQos& qos) { return qos.entity_factory(); },
            [](SubscriberQos& qos, const eprosima::fastdds::dds::EntityFactoryQosPolicy& entity_factory) {
                qos.entity_factory(entity_factory);
            });

    py::class_<SubscriberListener>(m, "SubscriberListener").def(py::init<>());

    py::class_<Subscriber>(m, "Subscriber").def("create_datareader", &Subscriber::create_datareader);

    py::class_<DomainParticipantQos>(m, "DomainParticipantQos")
        .def(py::init<>())
        // .def_property("user_data", [](const DomainParticipantQos &qos)
        //               { return qos.user_data().data_vec(); },
        //               [](DomainParticipantQos &qos, const
        //               std::vector<eprosima::fastrtps::rtps::octet> &data) {
        //               qos.user_data().data_vec(data); })
        // .def_property("entity_factory", [](const DomainParticipantQos &qos)
        //               { return qos.entity_factory(); }, [](DomainParticipantQos
        //               &qos, const EntityFactoryQosPolicy &entity_factory) {
        //               qos.entity_factory() = entity_factory; })
        // .def_property("partition", [](const DomainParticipantQos &qos)
        //               { return qos.partition(); }, [](DomainParticipantQos
        //               &qos, const PartitionQosPolicy &partition) {
        //               qos.partition() = partition; })
        // .def_property("wire_protocol", [](const DomainParticipantQos &qos)
        //               { return qos.wire_protocol(); }, [](DomainParticipantQos
        //               &qos, const WireProtocolConfigQos &wire_protocol) {
        //               qos.wire_protocol() = wire_protocol; })
        // .def_property("transport", [](const DomainParticipantQos &qos)
        //               { return qos.transport(); }, [](DomainParticipantQos
        //               &qos, const TransportConfigQos &transport) {
        //               qos.transport() = transport; })
        .def_property(
            "name", [](const DomainParticipantQos& qos) { return qos.name(); },
            [](DomainParticipantQos& qos, const std::string& name) { qos.name(name); })
        .def("properties", [](DomainParticipantQos& qos) { return py::cast(qos.properties()); })
        .def("flow_controllers", [](DomainParticipantQos& qos) { return py::cast(qos.flow_controllers()); });

    py::class_<eprosima::fastrtps::rtps::ParticipantDiscoveryInfo>(m, "ParticipantDiscoveryInfo")
        .def(py::init<>())
        .def_readwrite("status", &eprosima::fastrtps::rtps::ParticipantDiscoveryInfo::status)
        .def_readwrite("info", &eprosima::fastrtps::rtps::ParticipantDiscoveryInfo::info);

    py::class_<DomainParticipantListener, PyDomainParticipantListener, std::shared_ptr<DomainParticipantListener>>(
        m, "DomainParticipantListener")
        .def(py::init<>())
        .def("on_participant_discovery", [](DomainParticipantListener& self, DomainParticipant* participant, const eprosima::fastdds){})
        // .def("on_subscriber_discovery",
        //      py::overload_cast<DomainParticipant*, eprosima::fastrtps::rtps::ReaderDiscoveryInfo&&>(
        //          &DomainParticipantListener::on_subscriber_discovery))
        // .def("on_publisher_discovery",
        //      py::overload_cast<DomainParticipant*, eprosima::fastrtps::rtps::WriterDiscoveryInfo&&>(
        //          &DomainParticipantListener::on_publisher_discovery))
        .def("on_type_discovery", &DomainParticipantListener::on_type_discovery)
        .def("on_type_dependencies_reply", &DomainParticipantListener::on_type_dependencies_reply)
        .def("on_type_information_received", &DomainParticipantListener::on_type_information_received);

    py::class_<DomainParticipant>(m, "DomainParticipant")
        .def("create_publisher", &DomainParticipant::create_publisher, py::arg("qos") = PUBLISHER_QOS_DEFAULT,
             py::arg("listener").none(true) = py::none(), py::arg("mask") = StatusMask::all())
        .def("create_subscriber", &DomainParticipant::create_subscriber, py::arg("qos") = SUBSCRIBER_QOS_DEFAULT,
             py::arg("listener") = py::none(), py::arg("mask") = StatusMask::all())
        .def("create_topic", &DomainParticipant::create_topic, py::arg("topic_name"), py::arg("type_name"),
             py::arg("qos") = TOPIC_QOS_DEFAULT, py::arg("listener").none(true) = py::none(),
             py::arg("mask") = StatusMask::all())
        .def("get_qos",
             static_cast<const DomainParticipantQos& (DomainParticipant::*)() const>(&DomainParticipant::get_qos))
        .def("set_qos", &DomainParticipant::set_qos)
        .def("get_default_publisher_qos", static_cast<const PublisherQos& (DomainParticipant::*)() const>(
                                              &DomainParticipant::get_default_publisher_qos))
        .def("get_listener", &DomainParticipant::get_listener, py::return_value_policy::reference_internal)
        .def("set_listener",
             py::overload_cast<DomainParticipantListener*, const StatusMask&>(&DomainParticipant::set_listener),
             py::arg("listener"), py::arg("mask") = StatusMask::all())
        .def("register_type", py::overload_cast<TypeSupport, const std::string&>(&DomainParticipant::register_type))
        .def("enable", &DomainParticipant::enable);

    py::class_<DomainParticipantFactory, std::unique_ptr<DomainParticipantFactory, py::nodelete>>(
        m, "DomainParticipantFactory")
        .def(py::init([]() { return DomainParticipantFactory::get_instance(); }))
        .def("create_participant", &DomainParticipantFactory::create_participant, py::arg("domain_id") = 0,
             py::arg("qos") = PARTICIPANT_QOS_DEFAULT, py::arg("listener").none(true) = py::none(),
             py::arg("mask") = StatusMask::all());
}