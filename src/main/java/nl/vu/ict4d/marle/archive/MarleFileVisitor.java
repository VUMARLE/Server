package nl.vu.ict4d.marle.archive;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

class MarleFileVisitor extends SimpleFileVisitor<Path> {
	private long size = 0;

	public MarleFileVisitor() {
	}

	@Override
	public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
			throws IOException {
		size += attrs.size();
		return FileVisitResult.CONTINUE;
	}

	/**
	 * @return the value
	 */
	public long getValue() {
		return size;
	}
}
