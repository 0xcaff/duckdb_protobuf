use prost_types::{DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorSet};

pub trait FileDescriptorSetExt {
    fn message_matching(&self, message_name: &str) -> Option<&DescriptorProto>;
    fn enum_matching(&self, enum_name: &str) -> Option<&EnumDescriptorProto>;
}

impl FileDescriptorSetExt for FileDescriptorSet {
    fn message_matching(&self, name: &str) -> Option<&DescriptorProto> {
        for file_descriptor in &self.file {
            let package_name = file_descriptor.package();

            for message_descriptor in &file_descriptor.message_type {
                if &format!("{}.{}", package_name, message_descriptor.name()) == name {
                    return Some(message_descriptor);
                }
            }
        }

        None
    }

    fn enum_matching(&self, enum_name: &str) -> Option<&EnumDescriptorProto> {
        for file_descriptor in &self.file {
            let package_name = file_descriptor.package();

            for enum_descriptor in &file_descriptor.enum_type {
                if &format!("{}.{}", package_name, enum_descriptor.name()) == enum_name {
                    return Some(enum_descriptor);
                }
            }
        }

        None
    }
}

pub trait FieldDescriptorProtoExt {
    fn fully_qualified_type_name(&self) -> Result<&str, String>;
}

impl FieldDescriptorProtoExt for FieldDescriptorProto {
    fn fully_qualified_type_name(&self) -> Result<&str, String> {
        let type_name = self.type_name();
        let (prefix, absolute_type_name) = type_name.split_at(1);
        if prefix != "." {
            return Err(format!("invalid type name: {}", type_name));
        }

        Ok(absolute_type_name)
    }
}
