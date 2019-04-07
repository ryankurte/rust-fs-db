
use std::{fs, io};
use std::path::{Path, PathBuf};
use std::marker::PhantomData;
use std::fmt::Debug;

extern crate serde;
use serde::{de::DeserializeOwned, Serialize};

extern crate serde_json;

/// A simple file system based key:value data store
pub struct FileStore<V> {
    dir: PathBuf,
    _v: PhantomData<V>,
}

#[derive(Debug)]
pub enum Error<E> {
    Io(io::Error),
    Inner(E),
}

impl <E> From<io::Error> for Error<E> {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}


impl <V, E>FileStore<V> 
where
    V: EncodeDecode<Value=V, Error=E> + Serialize + DeserializeOwned + Debug,
    E: Debug
{
    /// Create a new FileStore
    pub fn new<P: AsRef<Path>>(dir: P) -> Result<Self, Error<E>> {
        Ok(FileStore{
            dir: dir.as_ref().into(), 
            _v: PhantomData
        })
    }

    /// List all files in the database
    pub fn list(&mut self) -> Result<Vec<String>, Error<E>> {
        let mut names = vec![];

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name().into_string().unwrap();
            names.push(name);
        }

        Ok(names)
    }

    /// Load a file by name
    pub fn load<P: AsRef<Path>>(&mut self, name: P) -> Result<V, Error<E>> {
        let mut path = self.dir.clone();
        path.push(name);

        let buff = fs::read(path)?;
        let obj: V = V::decode(&buff).map_err(|e| Error::Inner(e) )?;

        Ok(obj)
    }

    /// Store a file by name
    pub fn store<P: AsRef<Path>>(&mut self, name: P, v: &V) -> Result<(), Error<E>> {
        let mut path = self.dir.clone();
        path.push(name);
        
        let bin: Vec<u8> = V::encode(v).map_err(|e| Error::Inner(e) )?;
        fs::write(path, bin)?;
        Ok(())
    }

    /// Load all files from the database
    pub fn load_all(&mut self) -> Result<Vec<(String, V)>, Error<E>> {
        let mut objs = vec![];

        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name().into_string().unwrap();

            let buff = fs::read(entry.path())?;
            let obj: V = V::decode(&buff).map_err(|e| Error::Inner(e) )?;

            objs.push((name, obj));
        }

        Ok(objs)
    }

    /// Store a colection of files in the database
    pub fn store_all(&mut self, data: &[(String, V)]) -> Result<(), Error<E>> {
        for (name, value) in data {
            self.store(name, value)?;
        }

        Ok(())
    }


    /// Remove a file from the database
    pub fn rm<P: AsRef<Path>>(&mut self, name: P) -> Result<(), Error<E>> {
        let mut path = self.dir.clone();
        path.push(name);

        fs::remove_file(path)?;

        Ok(())
    }

}

/// EncodeDecode trait must be implemented for FileStore types
pub trait EncodeDecode {
    type Value;
    type Error;

    fn encode(value: &Self::Value) -> Result<Vec<u8>, Self::Error>;
    fn decode(buff: &[u8]) -> Result<Self::Value, Self::Error>;
}

/// Automagic EncodeDecode implementation for serde capable types
impl <V> EncodeDecode for V
where
    V: Serialize + DeserializeOwned + Debug,
{
    type Value = V;
    type Error = serde_json::Error;

    fn encode(value: &Self::Value) -> Result<Vec<u8>, Self::Error> {
        serde_json::to_vec(value)
    }

    fn decode(buff: &[u8]) -> Result<Self::Value, Self::Error> {
        serde_json::from_slice(&buff)
    }
}


#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    const N: usize = 3;

    #[test]
    fn mock_database() {

        let dir = env::temp_dir();

        let mut s = FileStore::new(dir).unwrap();

        for i in 0..N {
            let name = format!("{}", i);

            s.store(&name, &i).unwrap();

            let v = s.load(&name).unwrap();

            assert_eq!(i, v);
        }
    }
}
