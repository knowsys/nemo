// This is the interface to the JVM that we'll
// call the majority of our methods on.
use jni::JNIEnv;

// These objects are what you should use as arguments to your native function.
// They carry extra lifetime information to prevent them escaping from the
// current local frame (which is the scope within which local (temporary)
// references to Java objects remain valid)
use jni::objects::{GlobalRef, JClass, JObject, JString};
use jni::sys::jlong;
use stage2::io::parser::{all_input_consumed, RuleParser};
use stage2::logical::model::Program;
use stage2::physical::dictionary::Dictionary;
use std::fs::read_to_string;

#[cfg(feature = "no-prefixed-string-dictionary")]
type Dict = stage2::physical::dictionary::StringDictionary;
#[cfg(not(feature = "no-prefixed-string-dictionary"))]
type Dict = stage2::physical::dictionary::PrefixedStringDictionary;

struct MyState<Dict: Dictionary> {
    program: Option<Program<Dict>>,
}

impl<Dict: Dictionary> MyState<Dict> {
    pub fn parse(&mut self, filename: &str) {
        let parser = RuleParser::new();
        let program = all_input_consumed(parser.parse_program())(filename).unwrap();
        self.program = Some(program);
    }
}

#[no_mangle]
pub unsafe extern "system" fn Java_Stage2_create() -> jlong {
    let mystate = MyState::<Dict> { program: None };
    Box::into_raw(Box::new(mystate)) as jlong
}

#[no_mangle]
pub unsafe extern "system" fn Java_Stage2_parseRule<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    mystate_ptr: jlong,
    file: JString<'local>,
) {
    let mystate = &mut *(mystate_ptr as *mut MyState<Dict>);
    let file: String = env
        .get_string(&file)
        .expect("Couldn't get java string!")
        .into();
    let input = read_to_string(file).unwrap();
    mystate.parse(&input);
}
