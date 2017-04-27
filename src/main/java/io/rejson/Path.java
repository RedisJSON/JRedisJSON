package io.rejson;

/**
 * Path is a ReJSON path, representing a valid path into an object
 * TODO: make path building even more fun
 */

public class Path {
    private final String strPath;

    public Path(final String strPath) {
        this.strPath = strPath;
    }

    /**
     * Makes a root path
     * @return the root path
     */
    public static Path RootPath() {
        return new Path(".");
    }

    @Override
    public String toString() {
        return strPath;
    }
}
