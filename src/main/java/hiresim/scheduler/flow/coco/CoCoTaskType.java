package hiresim.scheduler.flow.coco;

public enum CoCoTaskType {

    /*
     * Models the interference score values for type classes according to
     * https://github.com/camsas/firmament/blob/master/src/scheduling/flow/coco_cost_model.cc#L42
     */

    RABBIT(2) {

        @Override
        public int getTurtlePenalty() {
            return CoCoConstants.Penalty_Rabbit_Turtle();
        }

        @Override
        public int getSheepPenalty() {
            return CoCoConstants.Penalty_Rabbit_Sheep();
        }

        @Override
        public int getRabbitPenalty() {
            return CoCoConstants.Penalty_Rabbit_Rabbit();
        }

        @Override
        public int getDevilPenalty() {
            return CoCoConstants.Penalty_Rabbit_Devil();
        }

    },
    TURTLE(0) {

        @Override
        public int getTurtlePenalty() {
            return CoCoConstants.Penalty_Turtle_Any();
        }

        @Override
        public int getSheepPenalty() {
            return CoCoConstants.Penalty_Turtle_Any();
        }

        @Override
        public int getRabbitPenalty() {
            return CoCoConstants.Penalty_Turtle_Any();
        }

        @Override
        public int getDevilPenalty() {
            return CoCoConstants.Penalty_Turtle_Any();
        }

    },
    SHEEP(1) {

        @Override
        public int getTurtlePenalty() {
            return CoCoConstants.Penalty_Sheep_Turtle();
        }

        @Override
        public int getSheepPenalty() {
            return CoCoConstants.Penalty_Sheep_Sheep();
        }

        @Override
        public int getRabbitPenalty() {
            return CoCoConstants.Penalty_Sheep_Rabbit();
        }

        @Override
        public int getDevilPenalty() {
            return CoCoConstants.Penalty_Sheep_Devil();
        }

    },
    DEVIL(3) {

        @Override
        public int getTurtlePenalty() {
            return CoCoConstants.Penalty_Devil_Turtle();
        }

        @Override
        public int getSheepPenalty() {
            return CoCoConstants.Penalty_Devil_Sheep();
        }

        @Override
        public int getRabbitPenalty() {
            return CoCoConstants.Penalty_Devil_Rabbit();
        }

        @Override
        public int getDevilPenalty() {
            return CoCoConstants.Penalty_Devil_Devil();
        }

    },

    // If no specific selection has been made
    UNDEFINED(-1) {

        @Override
        public int getTurtlePenalty() {
            return 0;
        }

        @Override
        public int getSheepPenalty() {
            return 0;
        }

        @Override
        public int getRabbitPenalty() {
            return 0;
        }

        @Override
        public int getDevilPenalty() {
            return 0;
        }

    };

    public static CoCoTaskType[] TYPES = new CoCoTaskType[] { RABBIT, TURTLE, SHEEP, DEVIL };

    public static int getPenalty(CoCoTaskType origin, CoCoTaskType relative) {
        switch (relative) {

            case SHEEP:
                return origin.getSheepPenalty();
            case DEVIL:
                return origin.getDevilPenalty();
            case RABBIT:
                return origin.getRabbitPenalty();
            case TURTLE:
                return origin.getTurtlePenalty();

            default:
                return 0;
        }
    }


    public final int index;

    CoCoTaskType(int index) {
        this.index = index;
    }

    public int getInterferenceVectorIndex() {
        return index;
    }


    public abstract int getTurtlePenalty();

    public abstract int getSheepPenalty();

    public abstract int getRabbitPenalty();

    public abstract int getDevilPenalty();


    public int getPenalty(CoCoTaskType relative) {
        return getPenalty(this, relative);
    }

}
