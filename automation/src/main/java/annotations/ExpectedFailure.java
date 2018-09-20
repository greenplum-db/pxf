package annotations;

import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Annotation for marking Test cases which Expected to fail.
 */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({ METHOD })
public @interface ExpectedFailure {
	// info is mandatory for explaining the failure reason
	String reason();
}
