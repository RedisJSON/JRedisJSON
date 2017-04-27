package io.rejson;

public class Path {
    private final String strPath;

    public Path(final String strPath) {
        this.strPath = strPath;
    }

    public static Path RootPath() {
        return new Path(".");
    }

    @Override
    public String toString() {
        return strPath;
    }
}
