package dev.buesing.ksd.streams;

import com.beust.jcommander.Parameter;
import dev.buesing.ksd.common.domain.args.VersionValidator;
import dev.buesing.ksd.tools.config.BaseOptions;
import java.util.Locale;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.RandomStringUtils;

@ToString
@Getter
@Setter
public class Options extends BaseOptions {

    @Parameter(names = {"-g", "--application-id"}, description = "application id")
    private String applicationId = "order-processor";

    @Parameter(names = {"-v", "--version"}, required = true, validateWith = VersionValidator.class, description = "application version v1 or v2")
    private String version;

    @Parameter(names = {"--client-id"}, description = "client id")
    private String clientId = RandomStringUtils.randomAlphabetic(4).toUpperCase(Locale.ROOT);

    @Parameter(names = {"--auto-offset-reset"}, description = "where to start consuming from if no offset is provided")
    private String autoOffsetReset = "earliest";

    @Parameter(names = { "--state-dir" }, description = "")
    private String stateDir = "./tmp";

    public String getVersionedApplicationId() {
        return applicationId + "-" + version;
    }

}


