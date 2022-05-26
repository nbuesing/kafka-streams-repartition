package dev.buesing.ksd.common.domain.args;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import java.util.regex.Pattern;

public class VersionValidator implements IParameterValidator {

    private static final String REGEX = "v1|v2";

    @Override
    public void validate(String name, String value) throws ParameterException {
        if (!isValid(value)) {
            throw new ParameterException("invalid version.");
        }
    }

    private boolean isValid(String value) {
        return Pattern.compile(REGEX).matcher(value).matches();
    }
}