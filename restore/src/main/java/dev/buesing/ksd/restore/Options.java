package dev.buesing.ksd.restore;

import com.beust.jcommander.Parameter;
import dev.buesing.ksd.common.domain.args.VersionValidator;
import dev.buesing.ksd.tools.config.BaseOptions;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class Options extends BaseOptions {

    @Parameter(names = { "--changelog-topic" }, description = "")
    private String changelogTopic= "order-processor-v1-product-purchased-store-changelog";

    @Parameter(names = { "--group-id" }, description = "")
    private String groupId= "order-processor-restore";

    @Parameter(names = { "--state-dir" }, description = "")
    private String stateDir = "./tmp";

    @Parameter(names = {"--from-version"}, validateWith = VersionValidator.class, description = "application version v1 or v2")
    private String fromVersion = "v1";

    @Parameter(names = {"--to-version"}, validateWith = VersionValidator.class, description = "application version v1 or v2")
    private String toVersion = "v2";


    public String getVersionedProductPurchasedRestore() {
        return this.getProductPurchasedRestore() + "-" + toVersion;
    }

}
