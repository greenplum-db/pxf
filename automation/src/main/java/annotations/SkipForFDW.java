package annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;

/**
 * Annotation for marking Test cases that should be skipped when run against PXF FDW.
 *
 * This is a marker interface for tests that cannot be run against FDW because they
 * exercise features of external tables that are not supported wth FDW.
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({ METHOD, TYPE })
public @interface SkipForFDW {
}
