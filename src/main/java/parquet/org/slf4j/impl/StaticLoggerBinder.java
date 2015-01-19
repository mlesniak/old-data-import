package parquet.org.slf4j.impl;

import parquet.org.slf4j.ILoggerFactory;
import parquet.org.slf4j.Logger;
import parquet.org.slf4j.Marker;

/**
 * This is an ugly hack to prevent error messages since slf4j-api is shaded in parquet. See
 * https://groups.google.com/forum/#!topic/parquet-dev/UjpbHbzoQj0
 *
 * Note that this will remove
 *
 * @author Michael Lesniak (mail@mlesniak.com)
 */
public class StaticLoggerBinder {
    private static StaticLoggerBinder singleton = new StaticLoggerBinder();

    public static parquet.org.slf4j.impl.StaticLoggerBinder getSingleton() {
        return singleton;
    }

    public parquet.org.slf4j.ILoggerFactory getLoggerFactory() {
        return new ILoggerFactory() {
            @Override
            public Logger getLogger(String className) {
                // If we are really bored we could delegate this to an org.slf4j.Logger for the given className.
                return new Logger() {

                    @Override
                    public String getName() {
                        return className;
                    }

                    @Override
                    public boolean isTraceEnabled() {
                        return false;
                    }

                    @Override
                    public void trace(String s) {

                    }

                    @Override
                    public void trace(String s, Object o) {

                    }

                    @Override
                    public void trace(String s, Object o, Object o1) {

                    }

                    @Override
                    public void trace(String s, Object... objects) {

                    }

                    @Override
                    public void trace(String s, Throwable throwable) {

                    }

                    @Override
                    public boolean isTraceEnabled(Marker marker) {
                        return false;
                    }

                    @Override
                    public void trace(Marker marker, String s) {

                    }

                    @Override
                    public void trace(Marker marker, String s, Object o) {

                    }

                    @Override
                    public void trace(Marker marker, String s, Object o, Object o1) {

                    }

                    @Override
                    public void trace(Marker marker, String s, Object... objects) {

                    }

                    @Override
                    public void trace(Marker marker, String s, Throwable throwable) {

                    }

                    @Override
                    public boolean isDebugEnabled() {
                        return false;
                    }

                    @Override
                    public void debug(String s) {

                    }

                    @Override
                    public void debug(String s, Object o) {

                    }

                    @Override
                    public void debug(String s, Object o, Object o1) {

                    }

                    @Override
                    public void debug(String s, Object... objects) {

                    }

                    @Override
                    public void debug(String s, Throwable throwable) {

                    }

                    @Override
                    public boolean isDebugEnabled(Marker marker) {
                        return false;
                    }

                    @Override
                    public void debug(Marker marker, String s) {

                    }

                    @Override
                    public void debug(Marker marker, String s, Object o) {

                    }

                    @Override
                    public void debug(Marker marker, String s, Object o, Object o1) {

                    }

                    @Override
                    public void debug(Marker marker, String s, Object... objects) {

                    }

                    @Override
                    public void debug(Marker marker, String s, Throwable throwable) {

                    }

                    @Override
                    public boolean isInfoEnabled() {
                        return false;
                    }

                    @Override
                    public void info(String s) {

                    }

                    @Override
                    public void info(String s, Object o) {

                    }

                    @Override
                    public void info(String s, Object o, Object o1) {

                    }

                    @Override
                    public void info(String s, Object... objects) {

                    }

                    @Override
                    public void info(String s, Throwable throwable) {

                    }

                    @Override
                    public boolean isInfoEnabled(Marker marker) {
                        return false;
                    }

                    @Override
                    public void info(Marker marker, String s) {

                    }

                    @Override
                    public void info(Marker marker, String s, Object o) {

                    }

                    @Override
                    public void info(Marker marker, String s, Object o, Object o1) {

                    }

                    @Override
                    public void info(Marker marker, String s, Object... objects) {

                    }

                    @Override
                    public void info(Marker marker, String s, Throwable throwable) {

                    }

                    @Override
                    public boolean isWarnEnabled() {
                        return false;
                    }

                    @Override
                    public void warn(String s) {

                    }

                    @Override
                    public void warn(String s, Object o) {

                    }

                    @Override
                    public void warn(String s, Object... objects) {

                    }

                    @Override
                    public void warn(String s, Object o, Object o1) {

                    }

                    @Override
                    public void warn(String s, Throwable throwable) {

                    }

                    @Override
                    public boolean isWarnEnabled(Marker marker) {
                        return false;
                    }

                    @Override
                    public void warn(Marker marker, String s) {

                    }

                    @Override
                    public void warn(Marker marker, String s, Object o) {

                    }

                    @Override
                    public void warn(Marker marker, String s, Object o, Object o1) {

                    }

                    @Override
                    public void warn(Marker marker, String s, Object... objects) {

                    }

                    @Override
                    public void warn(Marker marker, String s, Throwable throwable) {

                    }

                    @Override
                    public boolean isErrorEnabled() {
                        return false;
                    }

                    @Override
                    public void error(String s) {

                    }

                    @Override
                    public void error(String s, Object o) {

                    }

                    @Override
                    public void error(String s, Object o, Object o1) {

                    }

                    @Override
                    public void error(String s, Object... objects) {

                    }

                    @Override
                    public void error(String s, Throwable throwable) {

                    }

                    @Override
                    public boolean isErrorEnabled(Marker marker) {
                        return false;
                    }

                    @Override
                    public void error(Marker marker, String s) {

                    }

                    @Override
                    public void error(Marker marker, String s, Object o) {

                    }

                    @Override
                    public void error(Marker marker, String s, Object o, Object o1) {

                    }

                    @Override
                    public void error(Marker marker, String s, Object... objects) {

                    }

                    @Override
                    public void error(Marker marker, String s, Throwable throwable) {

                    }
                };
            }
        };
    }
}
