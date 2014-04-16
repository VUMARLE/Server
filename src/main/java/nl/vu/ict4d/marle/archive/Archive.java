package nl.vu.ict4d.marle.archive;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

import nl.vu.ict4d.marle.server.file.FileMeta;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

//handles saving files to the "database" which is a simple file folder
public class Archive {

	private static final Logger logger = Logger.getLogger("MarleLogger");

	private Path archivePath;

	// save a file
	// in save a file u must create a file metadata object and save it to the
	// JSON of the archive

	// get a file, by name or id
	// get all files/titles - search the JSON or content
	// calculate storage left - this is from the config file
	public Archive() throws IOException {

		archivePath = Paths.get("MarleArchive");

		logger.info("Archive located at: " + archivePath.toAbsolutePath());
		/*
		 * if (Files.notExists(archivePath)) { throw new
		 * NoSuchFileException(archivePath.toString()); }
		 */

	}

        /**
         * The path to the archive
         * @return The archive location on the filesystem
         */
	public Path getArchPath() {
		return archivePath;
	}
        
        /**
         * The path to the archive as a string
         * @return The archive location on the filesystem as a string
         */
	public String getArchPathString() {
		return archivePath.toString();
	}
        
        // -------------------------------------
        // Delete
        // -------------------------------------

        /**
         * Delets the file with the given name.
         * @param fileName
         * @throws IOException 
         */
	public void deleteFile(String fileName) throws IOException {
		Path filePath = archivePath.resolve(fileName);
		// this one will delete a file and if does not exist will not cry
		Files.deleteIfExists(filePath);
	}
        
        // -------------------------------------
        // Check if exists
        // -------------------------------------

	/**
         * This will check if a file with the given ID exists.
         * @param id
         * @throws NoSuchFileException 
         */
	public void checkIfExists(UUID id) throws NoSuchFileException {
		checkIfExists(id.toString());
	}

        /**
         * This will check if a file with the given ID exists.
         * @param fileName
         * @throws NoSuchFileException 
         */
	public void checkIfExists(String fileName) throws NoSuchFileException {
		if (Files.notExists(archivePath.resolve(fileName))) {
			throw new NoSuchFileException("The file " + fileName
					+ "doesn't exist");
		}
	}
        
        // -------------------------------------
        // Get file
        // -------------------------------------

	/**
         * Gives the file object of the file with the given id
         * @param id - the id of the file to lookup
         * @return 
         */
	public File getFileObject(UUID id) {
		return archivePath.resolve(id.toString()).toFile();
	}

        /**
         * Creates a byte array of the contents of the file
         * @param id
         * @return
         * @throws IOException 
         */
	public byte[] getFileBytes(UUID id) throws IOException {
		return getFileBytes(id.toString());
	}

        /**
         * Creates a byte array of the contents of the file
         * @param id
         * @return
         * @throws IOException 
         */
	public byte[] getFileBytes(String fileName) throws IOException {
		Path filePath = archivePath.resolve(fileName);
		return getFileBytes(filePath);
	}

	/**
	 * If the path is know, do not resolve it again!
	 */
	private byte[] getFileBytes(Path filePath) throws IOException {

		byte[] fileArray = null;
		try {
			fileArray = Files.readAllBytes(filePath);
		} catch (IOException ioex) {
			throw ioex;

		}
		return fileArray;
	}
        
        /**
         * Opens a new inputstream to the given file
         * @param id
         * @return 
         */
	public InputStream getFile(UUID id) throws IOException{
		return getFile(id.toString());
	}

        
        /**
         * Opens a new inputstream to the given file
         * @param id
         * @return 
         */
	public InputStream getFile(String fileName) throws IOException{

		// DataOutputStream file = null;
		Path filePath = archivePath.resolve(fileName);
                return Files.newInputStream(filePath);
//
//		try (InputStream file = Files.newInputStream(filePath)) {
//			return file;
//		} catch (IOException x) {
//			logger.info("Unable to create input stream for file: " + fileName);
//			return null;
//		}

	}
        
        // -------------------------------------
        // Get filesize
        // -------------------------------------

        /**
         * Looks up the size of the file in bytes
         * @param id
         * @return 
         */
	public long getFileSize(UUID id) {
		return archivePath.resolve(id.toString()).toFile().length();
	}

        // -------------------------------------
        // Save filesize
        // -------------------------------------
        
	public void saveFile(UUID id, byte[] fileContent) throws IOException {
		saveFile(id.toString(), fileContent);
	}

	public void saveFile(String fileName, byte[] fileContent)
			throws IOException {
		Path filePath = archivePath.resolve(fileName);
		saveFile(filePath, fileContent);

	}

	private void saveFile(Path filePath, byte[] fileContent) throws IOException {
		try {

			Files.createFile(filePath);
		} catch (FileAlreadyExistsException e) {
			System.err.println("already exists: " + e.getMessage());

			// We need to do this, as otherwise only the new bytes will be
			// written
			// That means that if the file gets smaller
			// The old bytes after the new length are NOT overwritten and kept.
			Files.deleteIfExists(filePath);
		}
		Files.write(filePath, fileContent, StandardOpenOption.WRITE,
				StandardOpenOption.CREATE);

	}

        // -------------------------------------
        // FileMeta
        // -------------------------------------

	private JSONArray fileToJSON(Path filePath) throws ParseException {
		byte[] fileContent = null;
		JSONArray fileContentJSON = null;
		try {
			fileContent = getFileBytes(filePath);
		} catch (NoSuchFileException ex) {
			// Create a new, as it does not exist yet
			fileContentJSON = new JSONArray();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (fileContentJSON == null) {
			JSONParser parser = new JSONParser();
			try {
				fileContentJSON = (JSONArray) parser.parse(new String(
						fileContent));
			} catch (ParseException pe) {
				pe.printStackTrace();
			}
		}
		return fileContentJSON;
	}
        
        /**
         * Looks up and returns the filemeta object for the given file.
         * @param fileid
         * @return
         * @throws IOException 
         */
        public FileMeta getFileMeta(UUID fileid) throws IOException {
            try {
		JSONArray contentJSON = getContentFile();
                JSONObject obj = null;
		for (int i = 0; i < contentJSON.size(); i++) {
			if (((JSONObject) contentJSON.get(i)).get("id").equals(fileid.toString())) {
				obj = (JSONObject) contentJSON.get(i);
				break;
			}
		}
                
                if (obj != null) {
                    try {
                        return FileMeta.fromJSON(obj);
                    } catch (java.text.ParseException ex) {
                        logger.error("Could not parse filemeta from the meta library!",ex);
                    }
                } 
            } catch (ParseException ex) {
                logger.error("Could not parse the meta library!",ex);
            }
            return null;
        }

	public void updateContentFile(FileMeta meta) throws IOException {
            try {
		Path filePath = archivePath.resolve("Content");
		JSONArray contentJSON = fileToJSON(filePath);
                
                // First check if the meta already exists
		for (int i = 0; i < contentJSON.size(); i++) {
			if (((JSONObject) contentJSON.get(i)).get("id").equals(
					meta.getId().toString())) {
				contentJSON.remove(i);
				break;
			}
		}
                // add the new meta
		contentJSON.add(meta.toJSON());
		saveFile(filePath, contentJSON.toJSONString().getBytes());

            } catch (ParseException ex) {
                logger.error("Could not parse the meta library! Failure to update filemeta, contents: \\" + meta.toJSON(), ex);
            }
	}

	public void removeMetaFromContentFile(UUID fileid) throws ParseException,
			IOException {
		Path filePath = archivePath.resolve("Content");
		JSONArray contentJSON = fileToJSON(filePath);
		for (int i = 0; i < contentJSON.size(); i++) {
			if (((JSONObject) contentJSON.get(i)).get("id").equals(
					fileid.toString())) {
				contentJSON.remove(i);
				break;
			}
		}
		saveFile(filePath, contentJSON.toJSONString().getBytes());

	}
        
	public JSONArray getContentFile() throws ParseException {
		Path filePath = archivePath.resolve("Content");
		return fileToJSON(filePath);
	}

        // -------------------------------------
        // Archive size
        // -------------------------------------

	// archive size calculation
	public long getArchiveSize() throws IOException {
                // TODO: Maybe make it cache this somewhere?
                // And make it update only when something is written etc.
		MarleFileVisitor test = new MarleFileVisitor();
		Files.walkFileTree(archivePath, test);
		return test.getValue();

	}
        
        /**
         * This method will lookup how much space is free in the archive.
         * If the archive size could not be determined, 0 will be returned.
         * @return The free space in the archive or 0 is an error occurred. 
         * Note that 0 can also be the actual free space in the archive!
         */
	public long getArchiveFreeSpace() {
            try {
                // For debugging purposes, the size is fixed to 512MB
                // TODO: get the actual size from the config file
                return (512 * 1024 * 1024) - getArchiveSize();
            } catch (IOException ex) {
                logger.fatal("Could not determine the amount of free space in the archive!");
                return 0;
            }
	}
}
